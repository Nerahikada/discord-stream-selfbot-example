import { Client, StageChannel } from "discord.js-selfbot-v13";
import type { Collection, GuildMember } from "discord.js-selfbot-v13";
import { Streamer, prepareStream, playStream } from "@dank074/discord-video-stream";
import { existsSync, readFileSync } from "node:fs";

interface Config {
    token: string;
    videos: Record<string, string>;
}

const config: Config = JSON.parse(readFileSync(new URL("../config.json", import.meta.url), "utf-8"));

if (!config.token) { console.error("Missing token in config.json"); process.exit(1); }
if (!config.videos || Object.keys(config.videos).length === 0) { console.error("No videos defined in config.json"); process.exit(1); }

const videoKeys = Object.keys(config.videos);

const client = new Client();
const streamer = new Streamer(client);

let controller: AbortController | null = null;
let activeChannelId: string | null = null;
let activeGuildId: string | null = null;
let activeVideoKey: string | null = null;

async function startStreaming(guildId: string, channelId: string, videoPath: string): Promise<void> {
    if (controller) return;

    const ac = new AbortController();
    controller = ac;
    activeGuildId = guildId;
    activeChannelId = channelId;

    try {
        await streamer.joinVoice(guildId, channelId);
        const joinedChannel = client.channels.cache.get(channelId);
        if (joinedChannel instanceof StageChannel) await client.user?.voice?.setSuppressed(false);
        console.log(`Joined voice channel ${channelId} in guild ${guildId}`);
    } catch (e) {
        console.error("Failed to join voice channel:", e);
        if (controller === ac) { controller = null; activeChannelId = null; activeGuildId = null; activeVideoKey = null; }
        return;
    }

    console.log(`Starting video playback: ${videoPath}`);
    try {
        const { output, promise } = prepareStream(videoPath, { noTranscoding: true, customInputOptions: ["-stream_loop", "-1"] }, ac.signal);
        await playStream(output, streamer, { type: "go-live" }, ac.signal);
        await promise.catch(() => {});
    } catch (e) {
        if (!ac.signal.aborted) console.error("Playback error:", e);
    } finally {
        if (controller === ac) { controller = null; activeChannelId = null; activeGuildId = null; activeVideoKey = null; }
    }
}

function stopStreaming(): void {
    if (!controller) return;
    console.log("Stopping stream...");
    controller.abort();
    controller = null;
    activeChannelId = null;
    activeGuildId = null;
    activeVideoKey = null;
    streamer.leaveVoice();
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

        const member = message.guild.members.cache.get(message.author.id);
        const voiceChannelId = member?.voice.channelId;
        if (!member || !voiceChannelId) {
            console.log(`!start from ${message.author.tag} — not in a voice channel, ignoring`);
            return;
        }
        if (controller) {
            console.log(`!start from ${message.author.tag} — already playing, ignoring`);
            return;
        }
        console.log(`!start ${key} from ${message.author.tag} — joining ${voiceChannelId}`);
        activeVideoKey = key;
        startStreaming(message.guild.id, voiceChannelId, videoPath);
    }

    if (lower === "!stop") {
        if (!controller || message.guild.id !== activeGuildId) return;
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
    if (!controller || !activeChannelId) return;

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

process.on("SIGINT", () => { stopStreaming(); process.exit(0); });
process.on("SIGTERM", () => { stopStreaming(); process.exit(0); });

client.login(config.token);
