import { Client, StageChannel } from "@lng2004/discord.js-selfbot-v13";
import type { Collection, GuildMember } from "@lng2004/discord.js-selfbot-v13";
import { Streamer, prepareStream, demux } from "@dank074/discord-video-stream";
import { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";

const _require = createRequire(import.meta.url);
const _libBase = _require.resolve("@dank074/discord-video-stream").replace(/dist\/index\.js$/, "");
const { VideoStream } = await import(_libBase + "dist/media/VideoStream.js");
const { AudioStream } = await import(_libBase + "dist/media/AudioStream.js");
const { AVCodecID } = await import(_libBase + "dist/media/LibavCodecId.js");

const videoCodecMap: Record<number, string> = { [AVCodecID.AV_CODEC_ID_H264]: "H264", [AVCodecID.AV_CODEC_ID_H265]: "H265", [AVCodecID.AV_CODEC_ID_VP8]: "VP8", [AVCodecID.AV_CODEC_ID_VP9]: "VP9", [AVCodecID.AV_CODEC_ID_AV1]: "AV1" };

const videos: Record<string, string> = JSON.parse(readFileSync(new URL("../videos.json", import.meta.url), "utf-8"));
const videoKeys = Object.keys(videos);
const token = process.env.DISCORD_TOKEN;

if (!token) { console.error("Missing DISCORD_TOKEN. Copy .env.example to .env and fill it in."); process.exit(1); }
if (videoKeys.length === 0) { console.error("No videos defined in videos.json"); process.exit(1); }

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
    // Without -readrate, ffmpeg reads a local file as fast as the disk allows, and node-av's demux thread busy-spins on setImmediate for as long as its packet queue stays full. Pacing ffmpeg at 1x keeps that queue from saturating.
    const { output, promise: ffmpegDone } = prepareStream(videoPath, { noTranscoding: true, customInputOptions: ["-stream_loop", "-1", "-readrate", "1", "-readrate_initial_burst", "1"] }, ac.signal);
    let demuxed: { video?: any; audio?: any } = {};

    try {
        demuxed = await demux(output, { format: "nut" });
        const { video, audio } = demuxed;
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
        video.stream.unpipe(vStream);
        if (aStream) audio.stream.unpipe(aStream);
    } catch (e) {
        if (!ac.signal.aborted) console.error("Playback error:", e);
    } finally {
        // The demuxer only tears itself down once it reads EOF, and it cannot reach that read until its packet queue is drained. Leaving a full queue behind keeps it spinning at 100% CPU forever.
        output.destroy();
        demuxed.video?.stream.resume();
        demuxed.audio?.stream.resume();
        await ffmpegDone.catch(() => {});
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
        const key = arg ?? videoKeys[0]!;
        const videoPath = videos[key];

        if (!videoPath) {
            console.log(`Ignoring !start from ${message.author.tag}: unknown key "${key}" (available: ${videoKeys.join(", ")})`);
            return;
        }
        if (!existsSync(videoPath)) {
            console.log(`Ignoring !start from ${message.author.tag}: file not found at ${videoPath}`);
            return;
        }

        const guild = message.guild;
        const member = guild.members.cache.get(message.author.id);
        const voiceChannelId = member?.voice.channelId;
        if (!member || !voiceChannelId) {
            console.log(`Ignoring !start from ${message.author.tag}: not in a voice channel`);
            return;
        }

        let sameChannel = false;
        if (activeVideoKey) {
            if (key === activeVideoKey) {
                console.log(`Ignoring !start from ${message.author.tag}: "${key}" is already playing`);
                return;
            }
            sameChannel = !!streamConn && voiceChannelId === activeChannelId;
            console.log(`Swapping from "${activeVideoKey}" to "${key}" at the request of ${message.author.tag}${sameChannel ? "" : " (changing channel)"}`);
            if (sameChannel) {
                playbackAbort?.abort();
            } else {
                stopStreaming();
            }
        } else {
            console.log(`Joining voice channel ${voiceChannelId} to play "${key}" at the request of ${message.author.tag}`);
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
            console.log(`Ignoring !stop from ${message.author.tag}: not in a voice channel`);
            return;
        }
        console.log(`Received !stop from ${message.author.tag}`);
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
    const humanMembers = members.filter((m: GuildMember) => m.id !== client.user?.id && !m.user.bot);
    if (humanMembers.size === 0) {
        console.log("Voice channel is empty, stopping...");
        stopStreaming();
    }
});

client.on("ready", () => {
    console.log(`Logged in as ${client.user?.tag}`);
    console.log(`Videos: ${videoKeys.map(k => `${k} → ${videos[k]}`).join(", ")}`);
    console.log("Commands: !start [key] / !stop");
});

function shutdown(): void { stopStreaming(); client.destroy(); setTimeout(() => process.exit(0), 2000).unref(); }
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

client.login(token);
