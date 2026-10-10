import { spawn } from "node:child_process";
import { PassThrough, pipeline } from "node:stream";
import type { Readable } from "node:stream";
import type { MediaTrack } from "./resolve.ts";

const ffmpegPath = process.env.FFMPEG_PATH ?? "ffmpeg";
// A single signed CDN URL runs past 1KB, and ffmpeg echoes the whole thing when an input fails, so the tail has to be roomy enough to still hold the lines around it.
const stderrTailChars = 8000;
const stderrTailLines = 4;
const stderrLineChars = 300;
// A refused input fails within a second of launch, so this only has to outlast a slow connect. Past it the stream is handed on as it is and the header timeout downstream takes over.
const refusalWindowMs = 10_000;

/** A copy-mux in flight. `refused` settles true only if ffmpeg died on a 403 from a progressive input before writing a single byte, and false as soon as that can no longer happen. */
export interface Remuxed { output: Readable; refused: Promise<boolean> }

function isHls(track: MediaTrack): boolean {
    return track.url.includes("m3u");
}

function headerArgs(headers: Record<string, string>): string[] {
    const entries = Object.entries(headers);
    return entries.length > 0 ? ["-headers", entries.map(([k, v]) => `${k}: ${v}`).join("\r\n")] : [];
}

// -readrate paces each HTTP read at 1x: without it ffmpeg drains the CDN as fast as the link allows and node-av's demux thread downstream busy-spins while its packet queue stays full.
function inputArgs(track: MediaTrack): string[] {
    const args = ["-readrate", "1", "-readrate_initial_burst", "5"];
    // The HLS demuxer retries segments on its own; ffmpeg's reconnect options target plain HTTP streams and interfere with playlist handling. Live sources arrive as m3u8.
    if (!isHls(track)) args.push("-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "30");
    return [...args, ...headerArgs(track.headers), "-i", track.url];
}

/** Copy-muxes a (video, audio) pair into a single stream so prepareStream can take it as one input. Killed when `signal` aborts. */
export function remux(video: MediaTrack, audio: MediaTrack | null, signal: AbortSignal): Remuxed {
    const args = ["-hide_banner", "-loglevel", "error", ...inputArgs(video)];
    if (audio) {
        args.push(...inputArgs(audio), "-map", "0:v:0", "-map", "1:a:0");
    } else {
        args.push("-map", "0:v:0", "-map", "0:a:0?");
    }
    // YouTube's h264 HLS renditions hand over video packets with no PTS and nut rejects them outright. DTS is there, so fill PTS in from it, and the guard leaves a packet that already has one untouched: forcing PTS=DTS unconditionally would reorder anything carrying B-frames.
    args.push("-bsf:v", "setts=pts=if(eq(PTS\\,NOPTS)\\,DTS\\,PTS)");
    // nut, not matroska: HLS carries AAC as ADTS with no out-of-band extradata, and the matroska muxer refuses to write a header without it, which is how every live source arrives.
    args.push("-c", "copy", "-f", "nut", "pipe:1");

    const child = spawn(ffmpegPath, args, { stdio: ["ignore", "pipe", "pipe"], signal });
    let stderrTail = "";

    // A long stream can log reconnect noise for hours, so keep only enough tail to explain a failure. setEncoding decodes through a StringDecoder, so a multi-byte character split across chunks survives.
    child.stderr.setEncoding("utf-8");
    child.stderr.on("data", (chunk: string) => { stderrTail = (stderrTail + chunk).slice(-stderrTailChars); });
    child.on("error", (e) => { if (!signal.aborted) console.error("Remux ffmpeg failed to start:", e); });
    // The transcoder closing its stdin first is normal teardown, and an unhandled EPIPE here would take the process down.
    child.stdout.on("error", () => {});
    child.on("close", (code) => {
        if (code === 0 || signal.aborted) return;
        // ffmpeg names the failing input on the lines before its own summary line, so the last line alone cannot tell a 403 on the video URL from one on the audio URL.
        const lines = stderrTail.trim().split("\n").map((line) => line.trim()).filter(Boolean).slice(-stderrTailLines).map((line) => (line.length > stderrLineChars ? `${line.slice(0, stderrLineChars)}…` : line));
        if (lines.length > 0) console.error(`Remux ffmpeg exited with ${code}:\n${lines.map((line) => `  ${line}`).join("\n")}`);
    });

    // Only a progressive googlevideo URL is refused per minting, which a fresh lookup can dodge. A 403 on HLS is missing auth or a geo-block and would fail the same way every time, so that path is handed over as it always was.
    if ([video, audio].every((track) => !track || isHls(track))) return { output: child.stdout, refused: Promise.resolve(false) };

    // Waiting on the verdict leaves stdout unread for a moment, and node discards whatever an exited child left unread: a source short enough to fit in the initial burst is gone before the consumer attaches. Piping from the start holds it instead.
    const output = new PassThrough();
    pipeline(child.stdout, output, () => {});
    const refused = new Promise<boolean>((resolve) => {
        const settle = (value: boolean) => { clearTimeout(timer); resolve(value); };
        const timer = setTimeout(() => settle(false), refusalWindowMs);
        // ffmpeg writes nothing until every input has opened, so a first byte means the headers were read and whatever fails later is not this refusal.
        child.stdout.once("data", () => settle(false));
        child.on("error", () => settle(false));
        child.on("close", (code) => settle(code !== 0 && !signal.aborted && stderrTail.includes("Server returned 403 Forbidden")));
    });

    return { output, refused };
}
