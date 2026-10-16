import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const ytdlpPath = process.env.YTDLP_PATH ?? "yt-dlp";
const resolveTimeoutMs = 30_000;

export interface MediaTrack { url: string; headers: Record<string, string> }

/** A resolved source. `audio` is null when the site serves one muxed file, in which case `video` carries both. */
export interface ResolvedMedia { video: MediaTrack; audio: MediaTrack | null; title: string; height: number | null; fps: number | null; isLive: boolean }

/** A playlist's entries, as page URLs still to be resolved one by one. `capped` says entries past the cap were left out. */
export interface ResolvedPlaylist { title: string; entries: { url: string; title: string | null }[]; capped: boolean }

async function runYtDlp(args: string[], signal?: AbortSignal): Promise<string> {
    try {
        // A full info dict runs to hundreds of KB (a plain YouTube video measured 660KB), so the 1MB default ceiling is within reach of a single lookup.
        const { stdout } = await execFileAsync(ytdlpPath, args, { timeout: resolveTimeoutMs, killSignal: "SIGKILL", maxBuffer: 64 * 1024 * 1024, signal });
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

function isHttpUrl(value: unknown): value is string {
    if (typeof value !== "string") return false;
    try { const { protocol } = new URL(value); return protocol === "http:" || protocol === "https:"; } catch { return false; }
}

/** Resolves a page URL to either directly streamable track URLs or, for a playlist, the first `maxEntries` of its entries, throwing with a readable reason on failure. */
export async function resolveTarget(target: string, maxHeight: number, maxEntries: number, signal?: AbortSignal): Promise<{ media: ResolvedMedia } | { playlist: ResolvedPlaylist }> {
    let parsed: URL;
    try { parsed = new URL(target); } catch { throw new Error(`not a URL: ${target}`); }
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new Error(`unsupported protocol: ${parsed.protocol}`);

    // `--flat-playlist` lists a playlist's entries without resolving each, and leaves a single video's lookup as it was. `--no-playlist` keeps a video URL that also names a playlist a single video. One entry past the cap is asked for only to tell whether the cap cut anything off, since `playlist_count` is missing for some playlists.
    // Sorting beats a fallback chain: `res` already prefers the tallest format within the cap and degrades on its own, and `vcodec` keeps the copy-mux a copy and the decode cheap.
    // `proto` last, to prefer HLS: a progressive googlevideo URL rejects ffmpeg's unbounded `Range: bytes=0-` with a 403 on roughly one lookup in seven, while HLS asks for one bounded segment at a time. Still only a preference, so a source without an HLS rendition degrades to progressive.
    const raw = await runYtDlp(["-J", "--flat-playlist", "--no-playlist", "--no-warnings", "-I", `1:${maxEntries + 1}`, "-f", "bv*+ba/b", "-S", `res:${maxHeight},vcodec:h264,proto:m3u8`, target], signal);
    if (!raw.trim()) throw new Error("yt-dlp returned no metadata");
    let info = JSON.parse(raw);

    // A page holding several videos lists them already extracted rather than as URLs, and re-resolving those direct CDN URLs later would drop the headers and cookies its extractor attached. So it plays its first video as served, the way it always has.
    const first = Array.isArray(info.entries) ? info.entries[0] : undefined;
    if (first && first._type !== "url" && first._type !== "url_transparent" && first._type !== "playlist") info = first;

    if (Array.isArray(info.entries)) {
        // Only a page URL can be resolved later: some extractors give a bare ID, and a nested playlist (a channel's tabs) gives none.
        const entries = info.entries.filter((e: any) => (e?._type === "url" || e?._type === "url_transparent") && isHttpUrl(e.url)).map((e: any) => ({ url: e.url as string, title: typeof e.title === "string" ? e.title : null }));
        return { playlist: { title: typeof info.title === "string" ? info.title : target, entries: entries.slice(0, maxEntries), capped: info.entries.length > maxEntries } };
    }

    // A video+audio pair arrives as requested_formats, one muxed file as the info dict itself. Treating the latter as a one-element list collapses both cases.
    const formats: any[] = Array.isArray(info.requested_formats) ? info.requested_formats : [info];
    // Codecs identify which track is which only when there is a choice to make. A lone format has to be taken as served: the generic extractor reports no vcodec at all for a direct file.
    const video = formats.length === 1 ? formats[0] : formats.find((f) => f.vcodec && f.vcodec !== "none");
    if (!video || typeof video.url !== "string") throw new Error("yt-dlp found no video stream for this URL");
    // Whatever is not the video track is the audio track. Its codec cannot be used to spot it either: YouTube's live audio renditions report none at all.
    const audio = formats.find((f) => f !== video) ?? null;

    return { media: { video: toTrack(video), audio: audio ? toTrack(audio) : null, title: typeof info.title === "string" ? info.title : target, height: toNumber(video.height), fps: toNumber(video.fps), isLive: info.is_live === true } };
}

/** Resolves a page URL to directly streamable track URLs via yt-dlp, throwing with a readable reason on failure or if the URL is a playlist. `signal` kills the lookup. */
export async function resolveMedia(target: string, maxHeight: number, signal?: AbortSignal): Promise<ResolvedMedia> {
    const resolved = await resolveTarget(target, maxHeight, 1, signal);
    if ("playlist" in resolved) throw new Error(`${target} is a playlist, not a single video`);
    return resolved.media;
}
