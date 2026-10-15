import { Client, StageChannel } from "@lng2004/discord.js-selfbot-v13";
import type { Collection, GuildMember, Message, VoiceState } from "@lng2004/discord.js-selfbot-v13";
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

/** One thing to play: `label` identifies it for dedupe and logs, `open` yields the ffmpeg input and may tie child processes to the playback's signal. `open` is awaited, so a source can take its time settling on an input that works. */
export interface PlaybackSource { label: string; open: (signal: AbortSignal) => string | Readable | Promise<string | Readable>; prepareOptions: Partial<PrepareStreamOptions> }

export interface RequestContext { command: string; requester: string }

/** Identity of one request. A fresh object per request, so an in-flight join can tell it has been superseded by comparing against the session's current one. */
interface ActiveRequest { channelId: string; label: string }

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

/** A `Streamer` that only hears its own guild's voice states. Every session of the account receives the others' voice states too, and the stock join takes the session ID from any of them, so without this a guild could connect with another session's ID. */
class GuildStreamer extends Streamer {
    constructor(client: Client, guildId: string) {
        super(client);
        // The listener the constructor just attached is the only way packets reach the Streamer, so wrapping it is enough.
        const forward = client.rawListeners("raw").at(-1) as (packet: any) => void;
        client.off("raw", forward);
        client.on("raw", (packet: any) => { if (packet.t === "VOICE_STATE_UPDATE" && packet.d?.guild_id !== guildId) return; forward(packet); });
    }
}

/** Owns the command client and routes each request to its guild's stream. A user account can be in only one voice channel per gateway session, so each guild streams from a session of its own, logged in with the same token. */
export class StreamSession {
    readonly client = new Client();
    // Never evicted: a guild's session stays logged in, so its next request joins without logging in again.
    private readonly guilds = new Map<string, GuildSession>();
    private token = "";

    /** Plays `source` in `guildId`, leaving every other guild's stream alone. */
    request(guildId: string, channelId: string, source: PlaybackSource, ctx: RequestContext): void {
        let session = this.guilds.get(guildId);
        if (!session) { session = new GuildSession(this.token, guildId, this.client.guilds.cache.get(guildId)?.name ?? guildId); this.guilds.set(guildId, session); }
        session.request(channelId, source, ctx);
    }

    /** Handles a `!stop` message by passing it to the stream in the guild it was sent from. */
    handleStop(message: Message): void {
        if (message.guild) this.guilds.get(message.guild.id)?.handleStop(message);
    }

    /** Wires up the voice-state guards and signal handlers, then logs in with `DISCORD_TOKEN`. */
    start(): void {
        const token = process.env.DISCORD_TOKEN;
        if (!token) { console.error("Missing DISCORD_TOKEN. Copy .env.example to .env and fill it in."); process.exit(1); }
        this.token = token;

        const shutdown = () => { for (const session of this.guilds.values()) session.destroy(); this.client.destroy(); setTimeout(() => process.exit(0), 2000).unref(); };
        process.on("SIGINT", shutdown);
        process.on("SIGTERM", shutdown);

        this.client.login(token);
    }
}

/** The single go-live stream one guild can have at a time, on a gateway session of its own. */
class GuildSession {
    private readonly client = new Client();
    private readonly token: string;
    private readonly guildId: string;
    private readonly streamer: GuildStreamer;
    private readonly logPrefix: string;
    private loggedIn: Promise<void> | null = null;
    private playbackAbort: AbortController | null = null;
    private streamConn: any = null;
    private packetizerCodec: string | null = null;
    private active: ActiveRequest | null = null;

    constructor(token: string, guildId: string, guildName: string) {
        this.token = token;
        this.guildId = guildId;
        this.streamer = new GuildStreamer(this.client, guildId);
        this.logPrefix = `[${guildName}]`;
        this.client.on("voiceStateUpdate", (oldState, newState) => this.handleVoiceState(oldState, newState));
    }

    /** Stops the stream and logs this guild's session out. */
    destroy(): void {
        this.stop();
        this.client.destroy();
    }

    /** Resolves once the session is ready to join. A failed login is forgotten, so the next request tries again. */
    private login(): Promise<void> {
        this.loggedIn ??= new Promise<void>((resolve, reject) => {
            this.client.once("ready", () => resolve());
            this.client.login(this.token).catch(reject);
        }).then(() => this.log(`Logged in a session for this guild as ${this.client.user?.tag}`), (e) => { this.loggedIn = null; throw e; });
        return this.loggedIn;
    }

    /** Plays `source`, joining or swapping as needed. Logs and ignores the request if it is already playing. */
    request(channelId: string, source: PlaybackSource, ctx: RequestContext): void {
        const previous = this.active;
        let sameChannel = false;

        if (previous) {
            if (source.label === previous.label) {
                this.log(`Ignoring ${ctx.command} from ${ctx.requester}: "${source.label}" is already playing`);
                return;
            }
            sameChannel = !!this.streamConn && channelId === previous.channelId;
            this.log(`Swapping from "${previous.label}" to "${source.label}" at the request of ${ctx.requester}${sameChannel ? "" : " (changing channel)"}`);
            if (sameChannel) {
                this.playbackAbort?.abort();
            } else {
                this.stop();
            }
        } else {
            this.log(`Joining voice channel ${channelId} to play "${source.label}" at the request of ${ctx.requester}`);
        }

        const active: ActiveRequest = { channelId, label: source.label };
        this.active = active;

        // Deliberately not awaited: both drive the stream in the background. The catch only exists so an unexpected throw cannot take the process down.
        if (sameChannel) {
            this.startPlayback(source).catch((e) => this.error("Playback failed:", e));
        } else {
            this.startStreaming(active, source).catch((e) => this.error("Streaming failed:", e));
        }
    }

    /** Handles a `!stop` message, ignoring it unless the author is listening in the channel being streamed to. */
    handleStop(message: Message): void {
        const active = this.active;
        if (!active || !message.guild) return;
        const member = message.guild.members.cache.get(message.author.id);
        if (member?.voice.channelId !== active.channelId) {
            this.log(`Ignoring !stop from ${message.author.tag}: not in the voice channel being streamed to`);
            return;
        }
        this.log(`Received !stop from ${message.author.tag}`);
        this.stop();
    }

    /** Tears everything down. The teardown itself runs unconditionally, so a half-finished join cannot leave the session connected, but it only narrates when there was something to stop. */
    stop(): void {
        const wasActive = this.active !== null;
        if (wasActive) this.log("Stopping stream...");
        this.playbackAbort?.abort();
        this.playbackAbort = null;
        if (this.streamConn) { this.streamer.stopStream(); this.streamConn = null; this.packetizerCodec = null; }
        this.streamer.leaveVoice();
        this.active = null;
        if (wasActive) this.log("Left voice channel.");
    }

    /** Stops when this session is moved or kicked out of the channel it streams to, or when that channel has no humans left. */
    handleVoiceState(oldState: VoiceState, newState: VoiceState): void {
        const active = this.active;
        if (!active) return;

        if (oldState.id === this.client.user?.id && oldState.channelId === active.channelId && newState.channelId !== active.channelId) {
            this.log("Bot was moved or kicked from voice channel, stopping...");
            this.stop();
            return;
        }

        if (oldState.channelId !== active.channelId) return;
        const channel = oldState.guild.channels.cache.get(active.channelId);
        if (!channel || !("members" in channel)) return;

        const members = channel.members as Collection<string, GuildMember>;
        const humanMembers = members.filter((m: GuildMember) => m.id !== this.client.user?.id && !m.user.bot);
        if (humanMembers.size === 0) {
            this.log("Voice channel is empty, stopping...");
            this.stop();
        }
    }

    private async startStreaming(active: ActiveRequest, source: PlaybackSource): Promise<void> {
        try {
            await this.login();
            // A stop() while logging in has nothing to undo yet, since no voice state was sent.
            if (this.active !== active) return;
            await this.streamer.joinVoice(this.guildId, active.channelId);
            // A stop() during the join already called leaveVoice, but this join landed after it, so undo it here. If something else is active instead, a newer request owns the connection and it must be left alone.
            if (this.active !== active) { if (!this.active) this.streamer.leaveVoice(); return; }
            const joinedChannel = this.client.channels.cache.get(active.channelId);
            // Not `client.user.voice`: that is whichever guild's voice state it finds first, which is the wrong one once several guilds are streaming.
            if (joinedChannel instanceof StageChannel) await joinedChannel.guild.members.me?.voice.setSuppressed(false);
            this.log(`Joined voice channel ${active.channelId}`);
        } catch (e) {
            this.error("Failed to join voice channel:", e);
            if (this.active === active) this.active = null;
            return;
        }

        // Held locally until the identity check passes, so a stale join cannot overwrite the connection a newer request already published.
        let conn: any;
        try {
            conn = await this.streamer.createStream();
        } catch (e) {
            this.error("Failed to create go-live stream:", e);
            if (this.active === active) { this.streamer.leaveVoice(); this.active = null; }
            return;
        }
        if (this.active !== active) { if (!this.active) { this.streamer.stopStream(); this.streamer.leaveVoice(); } return; }
        this.streamConn = conn;
        this.log("Go-live stream created");

        await this.startPlayback(source);
    }

    private async startPlayback(source: PlaybackSource): Promise<void> {
        const ac = new AbortController();
        this.playbackAbort = ac;

        this.log(`Starting video playback: ${source.label}`);

        // Both of these can throw, and they run before the teardown block below exists, so a failure here has to clean up on its own rather than becoming an unhandled rejection. A teardown that lands while `open` is still pending surfaces here too, as prepareStream refusing an aborted signal.
        let prepared: ReturnType<typeof prepareStream>;
        try {
            prepared = prepareStream(await source.open(ac.signal), source.prepareOptions, ac.signal);
        } catch (e) {
            if (!ac.signal.aborted) this.error("Failed to start playback:", e);
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
            this.log(`Sending ${video.width}x${video.height} ${codec} ${audio ? "with audio" : "with NO audio: the media carries no audio track"}`);

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
            if (!ac.signal.aborted) this.error("Playback error:", e);
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

    private log(...args: unknown[]): void { console.log(this.logPrefix, ...args); }

    private error(...args: unknown[]): void { console.error(this.logPrefix, ...args); }
}
