import { Client, StageChannel } from "@lng2004/discord.js-selfbot-v13";
import type { Collection, GuildMember, Message } from "@lng2004/discord.js-selfbot-v13";
import { Streamer, prepareStream, demux } from "@dank074/discord-video-stream";
import type { PrepareStreamOptions } from "@dank074/discord-video-stream";
import { createRequire } from "node:module";
import type { Readable } from "node:stream";

const _require = createRequire(import.meta.url);
const _libBase = _require.resolve("@dank074/discord-video-stream").replace(/dist\/index\.js$/, "");
const { VideoStream } = await import(_libBase + "dist/media/VideoStream.js");
const { AudioStream } = await import(_libBase + "dist/media/AudioStream.js");
const { AVCodecID } = await import(_libBase + "dist/media/LibavCodecId.js");

const videoCodecMap: Record<number, string | undefined> = { [AVCodecID.AV_CODEC_ID_H264]: "H264", [AVCodecID.AV_CODEC_ID_H265]: "H265", [AVCodecID.AV_CODEC_ID_VP8]: "VP8", [AVCodecID.AV_CODEC_ID_VP9]: "VP9", [AVCodecID.AV_CODEC_ID_AV1]: "AV1" };
// A header follows within a second of data actually flowing, so this only has to be loose enough to cover a slow start.
const headerTimeoutMs = 30_000;

/** One thing to play: `label` identifies it for dedupe and logs, `open` yields the ffmpeg input and may tie child processes to the playback's signal. */
export interface PlaybackSource { label: string; open: (signal: AbortSignal) => string | Readable; prepareOptions: Partial<PrepareStreamOptions> }

export interface RequestContext { command: string; requester: string }

/** Identity of one request. A fresh object per request, so an in-flight join can tell it has been superseded by comparing against the session's current one. */
interface ActiveRequest { guildId: string; channelId: string; label: string }

/** Returns the voice channel the message author sits in, or null (logging why) if they are not in one. */
export function voiceChannelOf(message: Message, command: string): string | null {
    const member = message.guild?.members.cache.get(message.author.id);
    const voiceChannelId = member?.voice.channelId;
    if (!voiceChannelId) {
        console.log(`Ignoring ${command} from ${message.author.tag}: not in a voice channel`);
        return null;
    }
    return voiceChannelId;
}

/** Awaits the demuxer, giving up if ffmpeg settles, the playback is torn down, or no header arrives in time. A source that never produces a readable header leaves `demux` pending forever and destroying its input is the only thing that wakes it, so every way the wait can stall needs its own exit.

A pending demuxer also holds a native thread that `process.exit` then deadlocks on, which is why giving up matters beyond this one playback: without it the whole bot stops responding to anything short of SIGKILL. */
async function demuxOrGiveUp(output: Readable, ffmpegDone: Promise<unknown>, signal: AbortSignal): Promise<Awaited<ReturnType<typeof demux>>> {
    let state: "waiting" | "demuxed" | "gave-up" = "waiting";
    let timer: ReturnType<typeof setTimeout> | undefined;
    const demuxed = demux(output, { format: "nut" });
    // Losing the race abandons the demuxer, and an abandoned one spins on its full packet queue forever, so a result that lands after the give-up still has to be drained.
    demuxed.then((d) => { if (state === "gave-up") { d.video?.stream.resume(); d.audio?.stream.resume(); return; } state = "demuxed"; clearTimeout(timer); }, () => clearTimeout(timer));

    const giveUp = new Promise<never>((_, reject) => {
        const fail = (reason: string) => { if (state !== "waiting") return; state = "gave-up"; clearTimeout(timer); output.destroy(); reject(new Error(reason)); };
        // A stalled source neither fails nor arrives, and nothing below would ever fire for it: ffmpeg happily waits on a connection that stays open and silent.
        timer = setTimeout(() => fail(`the media had no readable header after ${headerTimeoutMs}ms`), headerTimeoutMs);
        // Both outcomes count: ffmpeg rejects when it exits badly, but a source that ends before it ever had a header makes it resolve instead.
        ffmpegDone.then(() => fail("ffmpeg exited before the media had a readable header"), () => fail("ffmpeg failed before the media had a readable header"));
        if (signal.aborted) fail("playback was torn down before the media had a readable header");
        else signal.addEventListener("abort", () => fail("playback was torn down before the media had a readable header"), { once: true });
    });

    return Promise.race([demuxed, giveUp]);
}

/** Owns the selfbot client and the single go-live stream it can have at a time. */
export class StreamSession {
    readonly client = new Client();
    private readonly streamer = new Streamer(this.client);
    private playbackAbort: AbortController | null = null;
    private streamConn: any = null;
    private packetizerCodec: string | null = null;
    private active: ActiveRequest | null = null;

    /** Plays `source`, joining or swapping as needed. Logs and ignores the request if it is already playing. */
    request(guildId: string, channelId: string, source: PlaybackSource, ctx: RequestContext): void {
        const previous = this.active;
        let sameChannel = false;

        if (previous) {
            if (source.label === previous.label) {
                console.log(`Ignoring ${ctx.command} from ${ctx.requester}: "${source.label}" is already playing`);
                return;
            }
            sameChannel = !!this.streamConn && channelId === previous.channelId;
            console.log(`Swapping from "${previous.label}" to "${source.label}" at the request of ${ctx.requester}${sameChannel ? "" : " (changing channel)"}`);
            if (sameChannel) {
                this.playbackAbort?.abort();
            } else {
                this.stop();
            }
        } else {
            console.log(`Joining voice channel ${channelId} to play "${source.label}" at the request of ${ctx.requester}`);
        }

        const active: ActiveRequest = { guildId, channelId, label: source.label };
        this.active = active;

        // Deliberately not awaited: both drive the stream in the background. The catch only exists so an unexpected throw cannot take the process down.
        if (sameChannel) {
            this.startPlayback(source).catch((e) => console.error("Playback failed:", e));
        } else {
            this.startStreaming(active, source).catch((e) => console.error("Streaming failed:", e));
        }
    }

    /** Handles a `!stop` message, ignoring it unless the author is listening in the channel being streamed to. */
    handleStop(message: Message): void {
        const active = this.active;
        if (!active || !message.guild || message.guild.id !== active.guildId) return;
        const member = message.guild.members.cache.get(message.author.id);
        if (member?.voice.channelId !== active.channelId) {
            console.log(`Ignoring !stop from ${message.author.tag}: not in the voice channel being streamed to`);
            return;
        }
        console.log(`Received !stop from ${message.author.tag}`);
        this.stop();
    }

    /** Tears everything down. The teardown itself runs unconditionally, so a half-finished join cannot leave the session connected, but it only narrates when there was something to stop. */
    stop(): void {
        const wasActive = this.active !== null;
        if (wasActive) console.log("Stopping stream...");
        this.playbackAbort?.abort();
        this.playbackAbort = null;
        if (this.streamConn) { this.streamer.stopStream(); this.streamConn = null; this.packetizerCodec = null; }
        this.streamer.leaveVoice();
        this.active = null;
        if (wasActive) console.log("Left voice channel.");
    }

    /** Wires up the voice-state guards and signal handlers, then logs in with `DISCORD_TOKEN`. */
    start(): void {
        const token = process.env.DISCORD_TOKEN;
        if (!token) { console.error("Missing DISCORD_TOKEN. Copy .env.example to .env and fill it in."); process.exit(1); }

        this.client.on("voiceStateUpdate", (oldState, newState) => {
            const active = this.active;
            if (!active) return;

            if (oldState.id === this.client.user?.id && oldState.channelId === active.channelId && newState.channelId !== active.channelId) {
                console.log("Bot was moved or kicked from voice channel, stopping...");
                this.stop();
                return;
            }

            if (oldState.channelId !== active.channelId) return;
            const channel = oldState.guild.channels.cache.get(active.channelId);
            if (!channel || !("members" in channel)) return;

            const members = channel.members as Collection<string, GuildMember>;
            const humanMembers = members.filter((m: GuildMember) => m.id !== this.client.user?.id && !m.user.bot);
            if (humanMembers.size === 0) {
                console.log("Voice channel is empty, stopping...");
                this.stop();
            }
        });

        const shutdown = () => { this.stop(); this.client.destroy(); setTimeout(() => process.exit(0), 2000).unref(); };
        process.on("SIGINT", shutdown);
        process.on("SIGTERM", shutdown);

        this.client.login(token);
    }

    private async startStreaming(active: ActiveRequest, source: PlaybackSource): Promise<void> {
        try {
            await this.streamer.joinVoice(active.guildId, active.channelId);
            // A stop() during the join already called leaveVoice, but this join landed after it, so undo it here. If something else is active instead, a newer request owns the connection and it must be left alone.
            if (this.active !== active) { if (!this.active) this.streamer.leaveVoice(); return; }
            const joinedChannel = this.client.channels.cache.get(active.channelId);
            if (joinedChannel instanceof StageChannel) await this.client.user?.voice?.setSuppressed(false);
            console.log(`Joined voice channel ${active.channelId} in guild ${active.guildId}`);
        } catch (e) {
            console.error("Failed to join voice channel:", e);
            if (this.active === active) this.active = null;
            return;
        }

        // Held locally until the identity check passes, so a stale join cannot overwrite the connection a newer request already published.
        let conn: any;
        try {
            conn = await this.streamer.createStream();
        } catch (e) {
            console.error("Failed to create go-live stream:", e);
            if (this.active === active) { this.streamer.leaveVoice(); this.active = null; }
            return;
        }
        if (this.active !== active) { if (!this.active) { this.streamer.stopStream(); this.streamer.leaveVoice(); } return; }
        this.streamConn = conn;
        console.log("Go-live stream created");

        await this.startPlayback(source);
    }

    private async startPlayback(source: PlaybackSource): Promise<void> {
        const ac = new AbortController();
        this.playbackAbort = ac;

        console.log(`Starting video playback: ${source.label}`);

        // Both of these can throw synchronously, and they run before the teardown block below exists, so a failure here has to clean up on its own rather than becoming an unhandled rejection.
        let prepared: ReturnType<typeof prepareStream>;
        try {
            prepared = prepareStream(source.open(ac.signal), source.prepareOptions, ac.signal);
        } catch (e) {
            console.error("Failed to start playback:", e);
            ac.abort();
            if (this.playbackAbort === ac) this.stop();
            return;
        }

        const { output, promise: ffmpegDone } = prepared;
        let demuxed: { video?: any; audio?: any } = {};

        try {
            demuxed = await demuxOrGiveUp(output, ffmpegDone, ac.signal);
            const { video, audio } = demuxed;
            if (ac.signal.aborted || !this.streamConn) return;
            if (!video) throw new Error("No video stream in media");

            const codec = videoCodecMap[video.codec];
            if (!codec) throw new Error(`Unsupported video codec in media: ${video.codec}`);
            // setPacketizer cannot be called twice on one go-live: it resets the RTP sequence numbers of the reused native track, which trips SRTP anti-replay and segfaults. So a source with a different codec cannot join the live stream; stop instead, and the next request builds a go-live for it.
            if (this.packetizerCodec && this.packetizerCodec !== codec) throw new Error(`Live stream is packetized as ${this.packetizerCodec}, cannot switch to ${codec}. Stopping; run the command again to start a new stream.`);
            if (!this.packetizerCodec) {
                this.streamConn.setPacketizer(codec);
                this.streamConn.mediaConnection.setSpeaking(true);
                this.packetizerCodec = codec;
            }
            this.streamConn.mediaConnection.setVideoAttributes(true, { width: video.width, height: video.height, fps: Math.round(video.framerate_num / video.framerate_den) });

            // The go-live carries exactly what this reports, so "no audio" complaints can be placed above or below this line without reaching for ffmpeg: a media with no audio track never had any to send.
            console.log(`Sending ${video.width}x${video.height} ${codec} ${audio ? "with audio" : "with NO audio: the media carries no audio track"}`);

            const vStream = new VideoStream(this.streamConn);
            video.stream.pipe(vStream);
            let aStream: any = null;
            if (audio) {
                aStream = new AudioStream(this.streamConn);
                audio.stream.pipe(aStream);
                vStream.syncStream = aStream;
            }

            await new Promise<void>((resolve) => {
                const onAbort = () => { vStream.destroy(); if (aStream) aStream.destroy(); resolve(); };
                if (ac.signal.aborted) { onAbort(); return; }
                ac.signal.addEventListener("abort", onAbort, { once: true });
                vStream.once("finish", resolve);
            });
            video.stream.unpipe(vStream);
            if (aStream) audio.stream.unpipe(aStream);
        } catch (e) {
            if (!ac.signal.aborted) console.error("Playback error:", e);
        } finally {
            // Must come first: on the paths that get here without an abort (playback error, or a bail-out before streaming began) nothing else stops ffmpeg, and destroying `output` alone does not reliably make it exit. This also tears down whatever child processes `open` tied to the signal.
            ac.abort();
            // The demuxer only tears itself down once it reads EOF, and it cannot reach that read until its packet queue is drained. Leaving a full queue behind keeps it spinning at 100% CPU forever.
            output.destroy();
            demuxed.video?.stream.resume();
            demuxed.audio?.stream.resume();
            // Bounded by the abort above: execa escalates to SIGKILL 5s after the SIGTERM, so this settles even if ffmpeg ignores the signal.
            await ffmpegDone.catch(() => {});
            if (this.playbackAbort === ac) this.stop();
        }
    }
}
