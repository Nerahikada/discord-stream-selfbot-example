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

/** One thing to play: `label` identifies it for dedupe and logs, `title` is what the queue lists it as if known, `open` yields the ffmpeg input and may tie child processes to the playback's signal. `open` is awaited, so a source can take its time settling on an input that works. */
export interface PlaybackSource { label: string; title?: string; open: (signal: AbortSignal) => string | Readable | Promise<string | Readable>; prepareOptions: Partial<PrepareStreamOptions> }

export interface RequestContext { command: string; requester: string }

/** Identity of one request. A fresh object per request, so an in-flight join can tell it has been superseded by comparing against the session's current one. `source` follows the queue as it advances. */
interface ActiveRequest { channelId: string; source: PlaybackSource }

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

function describe(source: PlaybackSource): string {
    return source.title ? `"${source.title}" (${source.label})` : `"${source.label}"`;
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

/** A `Streamer` that only hears its own session's voice states. Every session of the account receives the others' voice states too, and the stock join takes the session ID from any of them, so without this a join could connect with another session's ID. */
class SessionStreamer extends Streamer {
    private readonly listener: (packet: any) => void;

    constructor(client: Client) {
        super(client);
        // The Streamer's only input is the raw listener its constructor just attached.
        const forward = client.rawListeners("raw").at(-1) as (packet: any) => void;
        client.off("raw", forward);
        this.listener = (packet: any) => { if (packet.t === "VOICE_STATE_UPDATE" && packet.d?.session_id !== client.sessionId) return; forward(packet); };
        client.on("raw", this.listener);
    }

    /** Stops packets reaching this Streamer, so a join or stream it left unfinished cannot react to the session's next borrower. */
    detach(): void { this.client.off("raw", this.listener); }
}

// Discord allows a user account 50 gateway sessions, and the command client holds one of them.
const maxStreamingSessions = 49;

/** Logged-in gateway sessions to stream from, lent out one per streaming guild. One spare is kept logged in so a request never waits on a login, and any session beyond that is logged out once returned. */
class SessionPool {
    private readonly idle: Client[] = [];
    private readonly all = new Set<Client>();
    private spare: Promise<Client> | null = null;
    private token = "";

    start(token: string): void {
        this.token = token;
        this.warm();
    }

    /** Lends out a ready session, then starts warming a replacement spare. */
    async acquire(): Promise<Client> {
        let client = this.idle.pop();
        if (!client) {
            const spare = this.spare;
            this.spare = null;
            client = await (spare ?? this.login());
        }
        this.warm();
        return client;
    }

    /** Takes back a session that has already left voice, keeping it as the spare or logging it out. */
    release(client: Client): void {
        if (this.idle.length > 0 || this.spare) { this.all.delete(client); client.destroy(); return; }
        this.idle.push(client);
    }

    /** Logs every session out, lent or not. */
    destroyAll(): void {
        for (const client of this.all) client.destroy();
        this.all.clear();
        this.idle.length = 0;
        this.spare = null;
    }

    private warm(): void {
        if (this.idle.length > 0 || this.spare || this.all.size >= maxStreamingSessions) return;
        const spare = this.login();
        this.spare = spare;
        spare.catch((e) => { if (this.spare === spare) this.spare = null; console.error("Failed to log in a spare streaming session:", e); });
    }

    private async login(): Promise<Client> {
        if (this.all.size >= maxStreamingSessions) throw new Error(`all ${maxStreamingSessions} streaming sessions are in use`);
        const client = new Client();
        this.all.add(client);
        try {
            await new Promise<void>((resolve, reject) => { client.once("ready", () => resolve()); client.login(this.token).catch(reject); });
        } catch (e) {
            this.all.delete(client);
            client.destroy();
            throw e;
        }
        console.log(`Logged in a streaming session (${this.all.size} logged in)`);
        return client;
    }
}

/** Owns the command client and routes each request to its guild's stream. A user account can be in only one voice channel per gateway session, so each streaming guild borrows a session of its own from the pool, all logged in with the same token. */
export class StreamSession {
    readonly client = new Client();
    private readonly pool = new SessionPool();
    // Never evicted: an idle GuildSession holds no session, only its guild's ID and name.
    private readonly guilds = new Map<string, GuildSession>();

    /** Plays `source` in `guildId`, leaving every other guild's stream alone. */
    request(guildId: string, channelId: string, source: PlaybackSource, ctx: RequestContext): void {
        this.guildSession(guildId).request(channelId, source, ctx);
    }

    /** Queues `source` behind whatever `guildId` is playing, or plays it at once if nothing is. */
    enqueue(guildId: string, channelId: string, source: PlaybackSource, ctx: RequestContext): void {
        this.guildSession(guildId).enqueue(channelId, source, ctx);
    }

    /** Handles a `!stop` message by passing it to the stream in the guild it was sent from. */
    handleStop(message: Message): void {
        this.guildFor(message)?.handleStop(message);
    }

    /** Handles a `!skip` message by passing it to the stream in the guild it was sent from. */
    handleSkip(message: Message): void {
        this.guildFor(message)?.handleSkip(message);
    }

    /** Logs what the guild a message was sent from is playing and what is queued behind it. */
    logQueue(message: Message, command: string): void {
        this.guildOf(message, command)?.logQueue(false);
    }

    /** Logs what the guild a message was sent from is playing. */
    logNowPlaying(message: Message, command: string): void {
        this.guildOf(message, command)?.logQueue(true);
    }

    /** Wires up the signal handlers, then logs the command client and the first spare streaming session in with `DISCORD_TOKEN`. */
    start(): void {
        const token = process.env.DISCORD_TOKEN;
        if (!token) { console.error("Missing DISCORD_TOKEN. Copy .env.example to .env and fill it in."); process.exit(1); }

        const shutdown = () => { for (const session of this.guilds.values()) session.stop(); this.pool.destroyAll(); this.client.destroy(); setTimeout(() => process.exit(0), 2000).unref(); };
        process.on("SIGINT", shutdown);
        process.on("SIGTERM", shutdown);

        this.client.login(token);
        this.pool.start(token);
    }

    /** Returns the stream of the guild a message was sent from, logging why not if it has none. */
    private guildOf(message: Message, command: string): GuildSession | undefined {
        const session = this.guildFor(message);
        if (!session) console.log(`Ignoring ${command} from ${message.author.tag}: nothing is playing`);
        return session;
    }

    private guildFor(message: Message): GuildSession | undefined {
        return message.guild ? this.guilds.get(message.guild.id) : undefined;
    }

    private guildSession(guildId: string): GuildSession {
        let session = this.guilds.get(guildId);
        if (!session) { session = new GuildSession(this.pool, guildId, this.client.guilds.cache.get(guildId)?.name ?? guildId); this.guilds.set(guildId, session); }
        return session;
    }
}

/** A session borrowed from the pool, with the Streamer and voice-state guard this guild put on it. `channelId` is where this session's own voice states last put it in the guild: the client's cache cannot say, since every session of the account overwrites the same entry. */
interface Lease { client: Client; streamer: SessionStreamer; onVoiceState: (oldState: VoiceState, newState: VoiceState) => void; channelId: string | null }

/** The single go-live stream one guild can have at a time, on a session borrowed from the pool for as long as it streams. */
class GuildSession {
    private readonly pool: SessionPool;
    private readonly guildId: string;
    private readonly logPrefix: string;
    // `leasing` is shared by every request racing for the session; `lease` is only set once it has resolved, and is what stop() tears down.
    private leasing: Promise<Lease> | null = null;
    private lease: Lease | null = null;
    private playbackAbort: AbortController | null = null;
    private streamConn: any = null;
    private packetizerCodec: string | null = null;
    private active: ActiveRequest | null = null;
    // What plays after `active.source`, in order. Emptied whenever the stream stops.
    private readonly queue: PlaybackSource[] = [];

    constructor(pool: SessionPool, guildId: string, guildName: string) {
        this.pool = pool;
        this.guildId = guildId;
        this.logPrefix = `[${guildName}]`;
    }

    /** Plays `source`, joining or swapping as needed. Logs and ignores the request if it is already playing. */
    request(channelId: string, source: PlaybackSource, ctx: RequestContext): void {
        const previous = this.active;
        let sameChannel = false;

        if (previous) {
            if (source.label === previous.source.label) {
                this.log(`Ignoring ${ctx.command} from ${ctx.requester}: "${source.label}" is already playing`);
                return;
            }
            sameChannel = !!this.streamConn && channelId === previous.channelId;
            this.log(`Swapping from "${previous.source.label}" to "${source.label}" at the request of ${ctx.requester}${sameChannel ? "" : " (changing channel)"}`);
            if (sameChannel) {
                this.playbackAbort?.abort();
            } else {
                // Keeps the session: rejoining on a different one would race this one's leave, which can land after the join and read as a kick.
                this.teardown(true);
            }
        } else {
            this.log(`Joining voice channel ${channelId} to play "${source.label}" at the request of ${ctx.requester}`);
        }

        const active: ActiveRequest = { channelId, source };
        this.active = active;

        // Deliberately not awaited: both drive the stream in the background. The catch only exists so an unexpected throw cannot take the process down.
        if (sameChannel) {
            this.startPlayback(source).catch((e) => this.error("Playback failed:", e));
        } else {
            this.startStreaming(active, source).catch((e) => this.error("Streaming failed:", e));
        }
    }

    /** Queues `source` behind whatever is playing, or plays it at once if nothing is. A request from outside the streamed channel is ignored, since nobody there would hear what it queues. */
    enqueue(channelId: string, source: PlaybackSource, ctx: RequestContext): void {
        const active = this.active;
        if (!active) { this.request(channelId, source, ctx); return; }
        if (channelId !== active.channelId) {
            this.log(`Ignoring ${ctx.command} from ${ctx.requester}: not in the voice channel being streamed to`);
            return;
        }
        this.queue.push(source);
        this.log(`Queued "${source.label}" at position ${this.queue.length} at the request of ${ctx.requester}`);
    }

    /** Logs what is playing and, unless `currentOnly`, everything queued behind it. */
    logQueue(currentOnly: boolean): void {
        const active = this.active;
        if (!active) { this.log("Nothing is playing"); return; }
        this.log(`Now playing: ${describe(active.source)}`);
        if (currentOnly) return;
        if (this.queue.length === 0) { this.log("The queue is empty"); return; }
        this.log(`Queue (${this.queue.length}):\n${this.queue.map((source, i) => `  ${i + 1}. ${describe(source)}`).join("\n")}`);
    }

    /** Handles a `!stop` message, ignoring it unless the author is listening in the channel being streamed to. */
    handleStop(message: Message): void {
        if (!this.fromListener(message, "!stop")) return;
        this.log(`Received !stop from ${message.author.tag}`);
        this.stop();
    }

    /** Handles a `!skip` message from a listener by moving on to the next queued source, stopping if there is none. */
    handleSkip(message: Message): void {
        const active = this.fromListener(message, "!skip");
        if (!active) return;
        // Before the go-live exists there is no playback to cut short, and playNext would stop rather than wait for it.
        if (!this.streamConn) {
            this.log(`Ignoring !skip from ${message.author.tag}: the stream is still starting`);
            return;
        }
        this.log(`Received !skip from ${message.author.tag}, skipping ${describe(active.source)}`);
        this.playbackAbort?.abort();
        this.playNext();
    }

    /** Returns what is playing if the message author is listening in the channel being streamed to, logging why not otherwise. Says nothing when nothing is playing. */
    private fromListener(message: Message, command: string): ActiveRequest | null {
        const active = this.active;
        if (!active || !message.guild) return null;
        const member = message.guild.members.cache.get(message.author.id);
        if (member?.voice.channelId !== active.channelId) {
            this.log(`Ignoring ${command} from ${message.author.tag}: not in the voice channel being streamed to`);
            return null;
        }
        return active;
    }

    /** Tears everything down and hands the session back to the pool. */
    stop(): void { this.teardown(false); }

    /** The teardown itself runs unconditionally, so a half-finished join cannot leave the session connected, but it only narrates when there was something to stop. With `keepLease`, the session leaves voice but stays borrowed for the request that is about to rejoin on it. */
    private teardown(keepLease: boolean): void {
        const wasActive = this.active !== null;
        if (wasActive) this.log("Stopping stream...");
        this.playbackAbort?.abort();
        this.playbackAbort = null;
        this.streamConn = null;
        this.packetizerCodec = null;
        this.forget();
        if (keepLease) this.leaveVoice(); else this.releaseLease();
        if (wasActive) this.log("Left voice channel.");
    }

    /** Drops what is playing and everything queued behind it. */
    private forget(): void {
        this.active = null;
        this.queue.length = 0;
    }

    /** Moves on to the next queued source on the same go-live, or stops once the queue has run out. */
    private playNext(): void {
        const active = this.active;
        const next = this.queue.shift();
        if (!active || !next || !this.streamConn) { this.stop(); return; }
        active.source = next;
        this.log(`Moving on to "${next.label}" (${this.queue.length} left in the queue)`);
        this.startPlayback(next).catch((e) => this.error("Playback failed:", e));
    }

    /** Stops when this session is moved or kicked out of the channel it streams to, or when that channel has no humans left. */
    handleVoiceState(oldState: VoiceState, newState: VoiceState): void {
        const active = this.active;
        const lease = this.lease;
        const self = lease?.client.user?.id;
        if (!active || !lease || !self) return;

        // Only this session's own voice states count: the guild's previous session keeps reporting its teardown, still in the channel, after a new one is borrowed.
        if (newState.id === self && newState.guild.id === this.guildId && newState.sessionId === lease.client.sessionId) {
            const previous = lease.channelId;
            lease.channelId = newState.channelId;
            if (previous === active.channelId && newState.channelId !== active.channelId) {
                this.log("Bot was moved or kicked from voice channel, stopping...");
                this.stop();
                return;
            }
        }

        if (oldState.channelId !== active.channelId) return;
        const channel = oldState.guild.channels.cache.get(active.channelId);
        if (!channel || !("members" in channel)) return;

        const members = channel.members as Collection<string, GuildMember>;
        const humanMembers = members.filter((m: GuildMember) => m.id !== self && !m.user.bot);
        if (humanMembers.size === 0) {
            this.log("Voice channel is empty, stopping...");
            this.stop();
        }
    }

    /** Borrows a session, or shares the borrow already in flight. A session that arrives after the request was stopped is returned at once. */
    private acquireLease(): Promise<Lease> {
        this.leasing ??= this.pool.acquire().then((client) => {
            const streamer = new SessionStreamer(client);
            const onVoiceState = (oldState: VoiceState, newState: VoiceState) => this.handleVoiceState(oldState, newState);
            client.on("voiceStateUpdate", onVoiceState);
            const lease: Lease = { client, streamer, onVoiceState, channelId: null };
            this.lease = lease;
            if (!this.active) this.releaseLease();
            return lease;
        }, (e) => { this.leasing = null; throw e; });
        return this.leasing;
    }

    /** Ends the go-live and leaves voice on the borrowed session, if one has arrived. */
    private leaveVoice(): void {
        // Not gated on streamConn: stopStream is a no-op without a stream, and also tears down one still being created.
        this.lease?.streamer.stopStream();
        this.lease?.streamer.leaveVoice();
    }

    /** Leaves voice on the borrowed session and returns it. A borrow still in flight is left to acquireLease, which returns it on arrival if nothing is active by then. */
    private releaseLease(): void {
        const lease = this.lease;
        if (!lease) return;
        this.leaveVoice();
        this.lease = null;
        this.leasing = null;
        lease.client.off("voiceStateUpdate", lease.onVoiceState);
        lease.streamer.detach();
        this.pool.release(lease.client);
    }

    // A flow that fails an identity check after an await returns without touching the lease: a stop() in between already tore it down and may have lent the session to another guild.
    private async startStreaming(active: ActiveRequest, source: PlaybackSource): Promise<void> {
        let lease: Lease;
        try {
            lease = await this.acquireLease();
        } catch (e) {
            this.error("Could not get a streaming session:", e);
            if (this.active === active) this.forget();
            return;
        }
        if (this.active !== active || this.lease !== lease) return;

        try {
            await lease.streamer.joinVoice(this.guildId, active.channelId);
            if (this.active !== active || this.lease !== lease) return;
            const joinedChannel = lease.client.channels.cache.get(active.channelId);
            // Not `client.user.voice`: that is whichever guild's voice state it finds first, and every session sees the account's voice states in all guilds.
            if (joinedChannel instanceof StageChannel) await joinedChannel.guild.members.me?.voice.setSuppressed(false);
            this.log(`Joined voice channel ${active.channelId}`);
        } catch (e) {
            this.error("Failed to join voice channel:", e);
            if (this.active === active && this.lease === lease) { this.forget(); this.releaseLease(); }
            return;
        }

        // Held locally until the identity check passes, so a stale join cannot overwrite the connection a newer request already published.
        let conn: any;
        try {
            conn = await lease.streamer.createStream();
        } catch (e) {
            this.error("Failed to create go-live stream:", e);
            if (this.active === active && this.lease === lease) { this.forget(); this.releaseLease(); }
            return;
        }
        if (this.active !== active || this.lease !== lease) return;
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
            if (this.playbackAbort === ac) this.playNext();
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
            if (this.playbackAbort === ac) this.playNext();
        }
    }

    private log(...args: unknown[]): void { console.log(this.logPrefix, ...args); }

    private error(...args: unknown[]): void { console.error(this.logPrefix, ...args); }
}
