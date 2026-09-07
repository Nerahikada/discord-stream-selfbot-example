import { Client, StageChannel } from "discord.js-selfbot-v13";
import type { Collection, GuildMember } from "discord.js-selfbot-v13";
import { Streamer, prepareStream, demux } from "@dank074/discord-video-stream";
import { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";

const _require = createRequire(import.meta.url);
const _libBase = _require.resolve("@dank074/discord-video-stream").replace(/dist\/index\.js$/, "");
const { VideoStream } = await import(_libBase + "dist/media/VideoStream.js");
const { AudioStream } = await import(_libBase + "dist/media/AudioStream.js");
const { AVCodecID } = await import(_libBase + "dist/media/LibavCodecId.js");

const videoCodecMap: Record<number, string> = { [AVCodecID.AV_CODEC_ID_H264]: "H264", [AVCodecID.AV_CODEC_ID_H265]: "H265", [AVCodecID.AV_CODEC_ID_VP8]: "VP8", [AVCodecID.AV_CODEC_ID_VP9]: "VP9", [AVCodecID.AV_CODEC_ID_AV1]: "AV1" };

interface Config { token: string; videos: Record<string, string> }

const config: Config = JSON.parse(readFileSync(new URL("../config.json", import.meta.url), "utf-8"));

if (!config.token) { console.error("Missing token in config.json"); process.exit(1); }
if (!config.videos || Object.keys(config.videos).length === 0) { console.error("No videos defined in config.json"); process.exit(1); }

const videoKeys = Object.keys(config.videos);
const client = new Client();
const streamer = new Streamer(client);

let playbackAbort: AbortController | null = null;
let activeChannelId: string | null = null;
let activeGuildId: string | null = null;
let activeVideoKey: string | null = null;
let streamConn: any = null;
let packetizerReady = false;

async function startStreaming(guildId: string, channelId: string, videoPath: string): Promise<void> {
    const expectedKey = activeVideoKey;

    try {
        await streamer.joinVoice(guildId, channelId);
        if (activeVideoKey !== expectedKey) return;
        const joinedChannel = client.channels.cache.get(channelId);
        if (joinedChannel instanceof StageChannel) await client.user?.voice?.setSuppressed(false);
        console.log(`Joined voice channel ${channelId} in guild ${guildId}`);
    } catch (e) {
        console.error("Failed to join voice channel:", e);
        if (activeVideoKey === expectedKey) { activeChannelId = null; activeGuildId = null; activeVideoKey = null; }
        return;
    }

    try {
        streamConn = await streamer.createStream();
        if (activeVideoKey !== expectedKey) { streamer.stopStream(); streamConn = null; return; }
        console.log("Go-live stream created");
    } catch (e) {
        console.error("Failed to create go-live stream:", e);
        if (activeVideoKey === expectedKey) { streamer.leaveVoice(); streamConn = null; activeChannelId = null; activeGuildId = null; activeVideoKey = null; }
        return;
    }

    startPlayback(videoPath);
}

async function startPlayback(videoPath: string): Promise<void> {
    const ac = new AbortController();
    playbackAbort = ac;

    console.log(`Starting video playback: ${videoPath}`);
    try {
        const { output, promise: ffmpegDone } = prepareStream(videoPath, { noTranscoding: true, customInputOptions: ["-stream_loop", "-1"] }, ac.signal);
        const { video, audio } = await demux(output, { format: "nut" });
        if (ac.signal.aborted || !streamConn) return;
        if (!video) throw new Error("No video stream in media");

        if (!packetizerReady) {
            streamConn.setPacketizer(videoCodecMap[video.codec]);
            streamConn.mediaConnection.setSpeaking(true);
            packetizerReady = true;
        }
        streamConn.mediaConnection.setVideoAttributes(true, { width: video.width, height: video.height, fps: Math.round(video.framerate_num / video.framerate_den) });

        const vStream = new VideoStream(streamConn);
        video.stream.pipe(vStream);
        let aStream: any = null;
        if (audio) {
            aStream = new AudioStream(streamConn);
            audio.stream.pipe(aStream);
            vStream.syncStream = aStream;
        }

        await new Promise<void>((resolve) => {
            const onAbort = () => { vStream.destroy(); if (aStream) aStream.destroy(); resolve(); };
            if (ac.signal.aborted) { onAbort(); return; }
            ac.signal.addEventListener("abort", onAbort, { once: true });
            vStream.once("finish", resolve);
        });
        await ffmpegDone.catch(() => {});
    } catch (e) {
        if (!ac.signal.aborted) console.error("Playback error:", e);
    } finally {
        if (playbackAbort === ac) stopStreaming();
    }
}

function stopStreaming(): void {
    console.log("Stopping stream...");
    playbackAbort?.abort();
    playbackAbort = null;
    if (streamConn) { streamer.stopStream(); streamConn = null; packetizerReady = false; }
    streamer.leaveVoice();
    activeChannelId = null; activeGuildId = null; activeVideoKey = null;
    console.log("Left voice channel.");
}

client.on("messageCreate", (message) => {
    if (message.author.id === client.user?.id) return;
    if (!message.guild) return;

    const content = message.content.trim();
    const lower = content.toLowerCase();

    if (lower.startsWith("!start")) {
        const arg = content.slice(6).trim() || null;
        const key = arg ?? videoKeys[0];
        const videoPath = config.videos[key];

        if (!videoPath) {
            console.log(`!start from ${message.author.tag} — unknown key "${key}", available: ${videoKeys.join(", ")}`);
            return;
        }
        if (!existsSync(videoPath)) {
            console.log(`!start from ${message.author.tag} — file not found: ${videoPath}`);
            return;
        }

        const guild = message.guild;
        const member = guild.members.cache.get(message.author.id);
        const voiceChannelId = member?.voice.channelId;
        if (!member || !voiceChannelId) {
            console.log(`!start from ${message.author.tag} — not in a voice channel, ignoring`);
            return;
        }

        let sameChannel = false;
        if (activeVideoKey) {
            if (key === activeVideoKey) {
                console.log(`!start from ${message.author.tag} — already playing "${key}", ignoring`);
                return;
            }
            sameChannel = !!streamConn && voiceChannelId === activeChannelId;
            console.log(`!start ${key} from ${message.author.tag} — swapping from "${activeVideoKey}"${sameChannel ? "" : " (changing channel)"}`);
            if (sameChannel) {
                playbackAbort?.abort();
            } else {
                stopStreaming();
            }
        } else {
            console.log(`!start ${key} from ${message.author.tag} — joining ${voiceChannelId}`);
        }

        activeGuildId = guild.id;
        activeChannelId = voiceChannelId;
        activeVideoKey = key;

        if (sameChannel) {
            startPlayback(videoPath);
        } else {
            startStreaming(guild.id, voiceChannelId, videoPath);
        }
    }

    if (lower === "!stop") {
        if (!activeVideoKey || message.guild.id !== activeGuildId) return;
        const member = message.guild.members.cache.get(message.author.id);
        if (!member?.voice.channelId) {
            console.log(`!stop from ${message.author.tag} — not in a voice channel, ignoring`);
            return;
        }
        console.log(`!stop from ${message.author.tag}`);
        stopStreaming();
    }
});

client.on("voiceStateUpdate", (oldState, newState) => {
    if (!activeVideoKey || !activeChannelId) return;

    if (oldState.id === client.user?.id && oldState.channelId === activeChannelId && newState.channelId !== activeChannelId) {
        console.log("Bot was moved or kicked from voice channel, stopping...");
        stopStreaming();
        return;
    }

    if (oldState.channelId !== activeChannelId) return;
    const channel = oldState.guild.channels.cache.get(activeChannelId);
    if (!channel || !("members" in channel)) return;

    const members = channel.members as Collection<string, GuildMember>;
    const otherMembers = members.filter((m: GuildMember) => m.id !== client.user?.id);
    if (otherMembers.size === 0) {
        console.log("Voice channel is empty, stopping...");
        stopStreaming();
    }
});

client.on("ready", () => {
    console.log(`Logged in as ${client.user?.tag}`);
    console.log(`Videos: ${videoKeys.map(k => `${k} → ${config.videos[k]}`).join(", ")}`);
    console.log("Commands: !start [key] / !stop");
});

function shutdown(): void { stopStreaming(); client.destroy(); setTimeout(() => process.exit(0), 2000).unref(); }
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

client.login(config.token);
