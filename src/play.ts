import { StreamSession, voiceChannelOf } from "./core/stream.ts";
import { resolveMedia } from "./core/resolve.ts";
import { remux } from "./core/remux.ts";

const maxHeight = 720;
const maxFps = 30;
const bitrateVideo = 2500;
const bitrateVideoMax = 4000;

const session = new StreamSession();

session.client.on("messageCreate", async (message) => {
    if (message.author.id === session.client.user?.id) return;
    if (!message.guild) return;

    const content = message.content.trim();
    const lower = content.toLowerCase();

    if (lower === "!play" || lower.startsWith("!play ")) {
        const target = content.slice(5).trim();
        if (!target) {
            console.log(`Ignoring !play from ${message.author.tag}: no URL given`);
            return;
        }

        const voiceChannelId = voiceChannelOf(message, "!play");
        if (!voiceChannelId) return;

        let media;
        try {
            media = await resolveMedia(target, maxHeight);
        } catch (e) {
            console.log(`Ignoring !play from ${message.author.tag}: ${e instanceof Error ? e.message : e}`);
            return;
        }
        console.log(`Resolved "${media.title}"${media.height ? ` (${media.height}p${media.fps ? Math.round(media.fps) : ""})` : ""}${media.isLive ? " [live]" : ""} for ${message.author.tag}`);

        // Only ever cap, never pad: leaving height/frameRate unset keeps the source as-is, so an unknown or already-small source is not upscaled into wasted encoding.
        const height = media.height && media.height > maxHeight ? maxHeight : undefined;
        const frameRate = media.fps && media.fps > maxFps ? maxFps : undefined;

        // Arbitrary sources never satisfy the no-B-frame / 1s-keyframe layout Discord needs, so unlike the file bot this one has to transcode.
        const prepareOptions = { noTranscoding: false, height, frameRate, videoCodec: "H264" as const, bitrateVideo, bitrateVideoMax };

        // The resolved CDN URLs are signed and change every lookup, so the page URL is what identifies this request.
        session.request(message.guild.id, voiceChannelId, { label: target, open: (signal) => remux(media.video, media.audio, signal), prepareOptions }, { command: "!play", requester: message.author.tag });
    }

    if (lower === "!stop") session.handleStop(message);
});

session.client.on("ready", () => {
    console.log(`Logged in as ${session.client.user?.tag}`);
    console.log("Commands: !play <URL> / !stop");
});

session.start();
