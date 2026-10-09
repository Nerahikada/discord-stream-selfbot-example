import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const ytdlpPath = process.env.YTDLP_PATH ?? "yt-dlp";
const resolveTimeoutMs = 30_000;

export interface MediaTrack { url: string; headers: Record<string, string> }

/** A resolved source. `audio` is null when the site serves one muxed file, in which case `video` carries both. */
export interface ResolvedMedia { video: MediaTrack; audio: MediaTrack | null; title: string; height: number | null; fps: number | null; isLive: boolean }

async function runYtDlp(args: string[]): Promise<string> {
    try {
        // A full info dict runs to hundreds of KB (a plain YouTube video measured 660KB), so the 1MB default ceiling is within reach of a single lookup.
        const { stdout } = await execFileAsync(ytdlpPath, args, { timeout: resolveTimeoutMs, killSignal: "SIGKILL", maxBuffer: 64 * 1024 * 1024 });
        return stdout;
    } catch (e) {
        const err = e as NodeJS.ErrnoException & { killed?: boolean; stderr?: string };
        if (err.killed) throw new Error(`yt-dlp timed out after ${resolveTimeoutMs}ms`);
        // A numeric code means yt-dlp ran and rejected the URL, so its own last line is the useful message; anything else failed before or around the process itself.
        if (typeof err.code === "number") throw new Error(`yt-dlp exited with ${err.code}: ${(err.stderr ?? "").trim().split("\n").at(-1) ?? ""}`);
        throw new Error(`could not run ${ytdlpPath}: ${err.message}`);
    }
}

// Only the attribute names yt-dlp actually emits: a wider list would start eating cookies that happen to be named after one.
const cookieAttributes = new Set(["domain", "path", "expires", "max-age", "secure", "httponly", "samesite"]);

/** Builds a `Cookie` header out of yt-dlp's per-format cookie string, which arrives Set-Cookie style with each pair's attributes inline after it. */
function cookieHeader(cookies: unknown): string | null {
    if (typeof cookies !== "string") return null;
    const pairs = cookies.split(";").map((part) => part.trim()).filter((part) => part.includes("=") && !cookieAttributes.has(part.split("=", 1)[0].trim().toLowerCase()));
    return pairs.length > 0 ? pairs.join("; ") : null;
}

function toTrack(format: any): MediaTrack {
    const headers: Record<string, string> = typeof format.http_headers === "object" && format.http_headers ? { ...format.http_headers } : {};
    // niconico's domand delivery keeps its auth out of http_headers and 403s every request that arrives without it, so the cookie jar has to be folded in here.
    const cookie = cookieHeader(format.cookies);
    if (cookie && !Object.keys(headers).some((name) => name.toLowerCase() === "cookie")) headers.Cookie = cookie;
    return { url: format.url, headers };
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
    // `proto` last, to prefer HLS: a progressive googlevideo URL rejects ffmpeg's unbounded `Range: bytes=0-` with a 403 on roughly one lookup in seven, while HLS asks for one bounded segment at a time. Still only a preference, so a source without an HLS rendition degrades to progressive.
    const raw = await runYtDlp(["-j", "--no-playlist", "--no-warnings", "-f", "bv*+ba/b", "-S", `res:${maxHeight},vcodec:h264,proto:m3u8`, target]);
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
