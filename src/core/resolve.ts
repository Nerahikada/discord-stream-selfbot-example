import { spawn } from "node:child_process";

const ytdlpPath = process.env.YTDLP_PATH ?? "yt-dlp";
const resolveTimeoutMs = 30_000;

export interface MediaTrack { url: string; headers: Record<string, string> }

/** A resolved source. `audio` is null when the site serves one muxed file, in which case `video` carries both. */
export interface ResolvedMedia { video: MediaTrack; audio: MediaTrack | null; title: string; height: number | null; fps: number | null; isLive: boolean }

function runYtDlp(args: string[]): Promise<string> {
    return new Promise((resolve, reject) => {
        const child = spawn(ytdlpPath, args, { stdio: ["ignore", "pipe", "pipe"] });
        const stdout: Buffer[] = [];
        const stderr: Buffer[] = [];
        const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error(`yt-dlp timed out after ${resolveTimeoutMs}ms`)); }, resolveTimeoutMs);

        child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
        child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
        child.on("error", (e) => { clearTimeout(timer); reject(new Error(`could not run ${ytdlpPath}: ${e.message}`)); });
        child.on("close", (code) => {
            clearTimeout(timer);
            if (code === 0) { resolve(Buffer.concat(stdout).toString("utf-8")); return; }
            const lastLine = Buffer.concat(stderr).toString("utf-8").trim().split("\n").at(-1) ?? "";
            reject(new Error(`yt-dlp exited with ${code}: ${lastLine}`));
        });
    });
}

function toTrack(format: any): MediaTrack {
    return { url: format.url, headers: typeof format.http_headers === "object" && format.http_headers ? format.http_headers : {} };
}

function toNumber(value: unknown): number | null {
    return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/** Resolves a page URL to directly streamable track URLs via yt-dlp, throwing with a readable reason on failure. */
export async function resolveMedia(target: string, maxHeight: number): Promise<ResolvedMedia> {
    let parsed: URL;
    try { parsed = new URL(target); } catch { throw new Error(`not a URL: ${target}`); }
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new Error(`unsupported protocol: ${parsed.protocol}`);

    // Sorting beats a fallback chain: `res` already prefers the tallest format within the cap and degrades on its own, and `vcodec` keeps the copy-mux a copy and the decode cheap.
    const raw = await runYtDlp(["-j", "--no-playlist", "--no-warnings", "-f", "bv*+ba/b", "-S", `res:${maxHeight},vcodec:h264`, target]);
    const line = raw.trim().split("\n")[0];
    if (!line) throw new Error("yt-dlp returned no metadata");
    const info = JSON.parse(line);

    // A video+audio pair arrives as requested_formats, one muxed file as the info dict itself. Treating the latter as a one-element list collapses both cases.
    const formats: any[] = Array.isArray(info.requested_formats) ? info.requested_formats : [info];
    // Codecs identify which track is which only when there is a choice to make. A lone format has to be taken as served: the generic extractor reports no vcodec at all for a direct file.
    const video = formats.length === 1 ? formats[0] : formats.find((f) => f.vcodec && f.vcodec !== "none");
    if (!video || typeof video.url !== "string") throw new Error("yt-dlp found no video stream for this URL");
    // Whatever is not the video track is the audio track. Its codec cannot be used to spot it either: YouTube's live audio renditions report none at all.
    const audio = formats.find((f) => f !== video) ?? null;

    return { video: toTrack(video), audio: audio ? toTrack(audio) : null, title: typeof info.title === "string" ? info.title : target, height: toNumber(video.height), fps: toNumber(video.fps), isLive: info.is_live === true };
}
