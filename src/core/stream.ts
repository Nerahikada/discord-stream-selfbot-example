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

const videoCodecMap: Record<number, string> = { [AVCodecID.AV_CODEC_ID_H264]: "H264", [AVCodecID.AV_CODEC_ID_H265]: "H265", [AVCodecID.AV_CODEC_ID_VP8]: "VP8", [AVCodecID.AV_CODEC_ID_VP9]: "VP9", [AVCodecID.AV_CODEC_ID_AV1]: "AV1" };

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

/** Owns the selfbot client and the single go-live stream it can have at a time. */
export class StreamSession {
    readonly client = new Client();
    private readonly streamer = new Streamer(this.client);
    private playbackAbort: AbortController | null = null;
    private streamConn: any = null;
    private packetizerReady = false;
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

    stop(): void {
        console.log("Stopping stream...");
        this.playbackAbort?.abort();
        this.playbackAbort = null;
        if (this.streamConn) { this.streamer.stopStream(); this.streamConn = null; this.packetizerReady = false; }
        this.streamer.leaveVoice();
        this.active = null;
        console.log("Left voice channel.");
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
            demuxed = await demux(output, { format: "nut" });
            const { video, audio } = demuxed;
            if (ac.signal.aborted || !this.streamConn) return;
            if (!video) throw new Error("No video stream in media");

            if (!this.packetizerReady) {
                this.streamConn.setPacketizer(videoCodecMap[video.codec]);
                this.streamConn.mediaConnection.setSpeaking(true);
                this.packetizerReady = true;
            }
            this.streamConn.mediaConnection.setVideoAttributes(true, { width: video.width, height: video.height, fps: Math.round(video.framerate_num / video.framerate_den) });

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
