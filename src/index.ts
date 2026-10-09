import { existsSync, readFileSync } from "node:fs";
import { StreamSession, voiceChannelOf } from "./core/stream.ts";

let videos: Record<string, string>;
try {
    videos = JSON.parse(readFileSync(new URL("../videos.json", import.meta.url), "utf-8"));
} catch (e) {
    console.error(`Could not read videos.json: ${e instanceof Error ? e.message : e}`);
    console.error("Copy videos.example.json to videos.json and fill it in.");
    process.exit(1);
}

const videoKeys = Object.keys(videos);

if (videoKeys.length === 0) { console.error("No videos defined in videos.json"); process.exit(1); }

const session = new StreamSession();

// Without -readrate, ffmpeg reads a local file as fast as the disk allows, and node-av's demux thread busy-spins on setImmediate for as long as its packet queue stays full. Pacing ffmpeg at 1x keeps that queue from saturating.
const prepareOptions = { noTranscoding: true, customInputOptions: ["-stream_loop", "-1", "-readrate", "1", "-readrate_initial_burst", "1"] };

session.client.on("messageCreate", (message) => {
    if (message.author.id === session.client.user?.id) return;
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

        const voiceChannelId = voiceChannelOf(message, "!start");
        if (!voiceChannelId) return;

        session.request(message.guild.id, voiceChannelId, { label: key, open: () => videoPath, prepareOptions }, { command: "!start", requester: message.author.tag });
    }

    if (lower === "!stop") session.handleStop(message);
});

session.client.on("ready", () => {
    console.log(`Logged in as ${session.client.user?.tag}`);
    console.log(`Videos: ${videoKeys.map(k => `${k} → ${videos[k]}`).join(", ")}`);
    console.log("Commands: !start [key] / !stop");
});

session.start();
