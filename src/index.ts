import { Client, StageChannel } from "discord.js-selfbot-v13";
import type { Collection, GuildMember } from "discord.js-selfbot-v13";
import { Streamer, prepareStream, playStream } from "@dank074/discord-video-stream";
import { existsSync } from "node:fs";

const TOKEN = process.env.DISCORD_TOKEN!;
const VIDEO_PATH = process.env.VIDEO_PATH!;

if (!TOKEN || !VIDEO_PATH) {
    console.error("Missing required env vars: DISCORD_TOKEN, VIDEO_PATH");
    process.exit(1);
}
if (!existsSync(VIDEO_PATH)) {
    console.error(`Video file not found: ${VIDEO_PATH}`);
    process.exit(1);
}

const client = new Client();
const streamer = new Streamer(client);

let controller: AbortController | null = null;
let activeChannelId: string | null = null;
let activeGuildId: string | null = null;

async function startStreaming(guildId: string, channelId: string): Promise<void> {
    if (controller) return;

    const ac = new AbortController();
    controller = ac;
    activeGuildId = guildId;
    activeChannelId = channelId;

    const delay = 2000 + Math.random() * 3000;
    await new Promise(r => setTimeout(r, delay));
    if (ac.signal.aborted) return;

    try {
        await streamer.joinVoice(guildId, channelId);
        const joinedChannel = client.channels.cache.get(channelId);
        if (joinedChannel instanceof StageChannel) await client.user?.voice?.setSuppressed(false);
        console.log(`Joined voice channel ${channelId} in guild ${guildId}`);
    } catch (e) {
        console.error("Failed to join voice channel:", e);
        if (controller === ac) { controller = null; activeChannelId = null; activeGuildId = null; }
        return;
    }

    console.log("Starting video playback...");
    try {
        const { output, promise } = prepareStream(VIDEO_PATH, {
            noTranscoding: true,
            customInputOptions: ["-stream_loop", "-1"],
        }, ac.signal);
        await playStream(output, streamer, { type: "go-live" }, ac.signal);
        await promise.catch(() => {});
    } catch (e) {
        if (!ac.signal.aborted) console.error("Playback error:", e);
    } finally {
        if (controller === ac) { controller = null; activeChannelId = null; activeGuildId = null; }
    }
}

function stopStreaming(): void {
    if (!controller) return;
    console.log("Stopping stream...");
    controller.abort();
    controller = null;
    activeChannelId = null;
    activeGuildId = null;
    streamer.leaveVoice();
    console.log("Left voice channel.");
}

client.on("messageCreate", (message) => {
    if (message.author.id === client.user?.id) return;
    if (!message.guild) return;

    const content = message.content.trim().toLowerCase();

    if (content === "!start") {
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
        console.log(`!start from ${message.author.tag} — joining ${voiceChannelId}`);
        startStreaming(message.guild.id, voiceChannelId);
    }

    if (content === "!stop") {
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
    console.log(`Video: ${VIDEO_PATH}`);
    console.log("Commands: !start / !stop (from any text channel)");
});

process.on("SIGINT", () => { stopStreaming(); process.exit(0); });
process.on("SIGTERM", () => { stopStreaming(); process.exit(0); });

client.login(TOKEN);
