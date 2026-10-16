import { StreamSession, voiceChannelOf } from "./core/stream.ts";
import type { LoopMode } from "./core/stream.ts";
import type { PlaybackSource } from "./core/stream.ts";
import { resolveMedia, resolveTarget } from "./core/resolve.ts";
import type { ResolvedMedia } from "./core/resolve.ts";
import { remux } from "./core/remux.ts";
import type { PrepareStreamOptions } from "@dank074/discord-video-stream";

const maxHeight = 720;
const maxFps = 30;
const bitrateVideo = 2500;
const bitrateVideoMax = 4000;
// Roughly one lookup in seven mints a refused URL, so two more draws leave about one playback in three hundred failing on it.
const maxFreshLookups = 2;
// Signed CDN URLs expire, so only a playback that starts this soon after its !play may reuse the lookup that validated it.
const lookupReuseMs = 5 * 60_000;
const maxPlaylistEntries = 100;

/** A source for the page URL `target`. `lookup` is the resolve that validated it, if one was made; without it, the first playback resolves on its own. */
function sourceFor(target: string, title: string | null, lookup: ResolvedMedia | null): PlaybackSource {
    // Arbitrary sources never satisfy the no-B-frame / 1s-keyframe layout Discord needs, so unlike the file bot this one has to transcode.
    const prepareOptions: Partial<PrepareStreamOptions> = { noTranscoding: false, videoCodec: "H264", bitrateVideo, bitrateVideoMax };

    // A refused progressive URL stays refused for as long as it lives, so reopening it is pointless: the only way past is a fresh lookup, which mints new ones. This has to happen in here because `open` runs once per playback.
    let reusable = lookup ? { media: lookup, at: Date.now() } : null;
    const open = async (signal: AbortSignal) => {
        let current = reusable && Date.now() - reusable.at < lookupReuseMs ? reusable.media : null;
        reusable = null;
        // Tied to the playback, so skipping through the queue does not leave a lookup running per skipped source.
        current ??= await resolveMedia(target, maxHeight, signal);
        signal.throwIfAborted();
        for (let lookups = 0; ; lookups++) {
            // Only ever cap, never pad: leaving height/frameRate unset keeps the source as-is, so an unknown or already-small source is not upscaled into wasted encoding.
            prepareOptions.height = current.height && current.height > maxHeight ? maxHeight : undefined;
            prepareOptions.frameRate = current.fps && current.fps > maxFps ? maxFps : undefined;
            const { output, refused } = remux(current.video, current.audio, signal);
            if (lookups >= maxFreshLookups || !(await refused)) return output;
            console.log(`The CDN refused the URLs resolved for "${target}", resolving again (${lookups + 1}/${maxFreshLookups})`);
            current = await resolveMedia(target, maxHeight, signal);
            signal.throwIfAborted();
        }
    };

    // The resolved CDN URLs are signed and change every lookup, so the page URL is what identifies this source.
    return { label: target, title: title ?? undefined, open, prepareOptions };
}

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

        let resolved;
        try {
            resolved = await resolveTarget(target, maxHeight, maxPlaylistEntries);
        } catch (e) {
            console.log(`Ignoring !play from ${message.author.tag}: ${e instanceof Error ? e.message : e}`);
            return;
        }

        let sources: PlaybackSource[];
        if ("playlist" in resolved) {
            const { playlist } = resolved;
            if (playlist.entries.length === 0) {
                console.log(`Ignoring !play from ${message.author.tag}: playlist "${playlist.title}" has no playable entries`);
                return;
            }
            // Entries are only looked up as they come up: a lookup takes seconds, and its signed URLs would expire long before the later entries play.
            console.log(`Resolved playlist "${playlist.title}" with ${playlist.entries.length} entries${playlist.capped ? ` (capped at ${maxPlaylistEntries})` : ""} for ${message.author.tag}`);
            sources = playlist.entries.map((entry) => sourceFor(entry.url, entry.title, null));
        } else {
            const { media } = resolved;
            console.log(`Resolved "${media.title}"${media.height ? ` (${media.height}p${media.fps ? Math.round(media.fps) : ""})` : ""}${media.isLive ? " [live]" : ""} for ${message.author.tag}`);
            sources = [sourceFor(target, media.title, media)];
        }

        session.enqueue(message.guild.id, voiceChannelId, sources, { command: "!play", requester: message.author.tag });
    }

    if (lower === "!stop") session.handleStop(message);
    if (lower === "!skip") session.handleSkip(message);
    if (lower === "!shuffle") session.handleShuffle(message);
    if (lower === "!loop" || lower.startsWith("!loop ")) {
        const arg = lower.slice(5).trim();
        if (arg && arg !== "off" && arg !== "one" && arg !== "all") {
            console.log(`Ignoring !loop from ${message.author.tag}: unknown mode "${arg}" (expected off, one or all)`);
            return;
        }
        session.handleLoop(message, (arg || null) as LoopMode | null);
    }
    if (lower === "!clear") session.handleClear(message);
    if (lower === "!remove" || lower.startsWith("!remove ")) {
        const arg = lower.slice(7).trim();
        if (!/^\d+$/.test(arg)) {
            console.log(`Ignoring !remove from ${message.author.tag}: expected a queue position, got "${arg}"`);
            return;
        }
        session.handleRemove(message, Number(arg));
    }
    if (lower === "!queue") session.logQueue(message, "!queue");
    if (lower === "!np") session.logNowPlaying(message, "!np");
});

session.client.on("ready", () => {
    console.log(`Logged in as ${session.client.user?.tag}`);
    console.log("Commands: !play <URL> / !stop / !skip / !shuffle / !loop [off|one|all] / !remove <n> / !clear / !queue / !np");
});

session.start();
