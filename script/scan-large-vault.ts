// Reproduce an offline scan of a large vault and report how many files were sent.
//
//   deno run -A script/scan-large-vault.ts [--count 70000] [--vault DIR] [--keep] [--timeout 600] [--name-length N]
//
// --name-length pads the peer name, which is part of every record key, so that a
// few thousand files are enough to pass the 10 MB limit.
//
// A vault of --count small files with long Japanese names is created (or reused
// with --vault), then a storage peer with scanOfflineChanges: true is started.
// Nothing is sent anywhere: the dispatcher only counts. A second peer instance
// then scans the same vault with the saved state and must send nothing.
//
// Before peer state moved to SQLite, the per-file records filled Deno's 10 MB
// localStorage after roughly 42,000 files and the rest were never sent.
import { join } from "@std/path";
import { parseArgs } from "@std/cli";
import { LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";
import { PeerStorage } from "../PeerStorage.ts";
import type { FileData, PeerStorageConf } from "../types.ts";

const flags = parseArgs(Deno.args, {
    string: ["count", "vault", "timeout", "name-length"],
    boolean: ["keep"],
    default: { count: "70000", timeout: "900", "name-length": "0" },
});
const count = Number(flags.count);
const timeoutMs = Number(flags.timeout) * 1000;

const work = await Deno.makeTempDir({ prefix: "lsb-scan-" });
const vault = flags.vault ?? join(work, "vault");
const stateFile = join(work, "state.sqlite");
// Older revisions of PeerState.ts read the location from the environment.
Deno.env.set("LSB_STATE_FILE", stateFile);

// PeerState.ts is absent on the revision that predates the fix.
const peerState = await import("../PeerState.ts").catch(() => undefined) as
    | { initPeerState?: (o: { filePath: string; legacy: undefined }) => Promise<unknown> }
    | undefined;
// Start from an empty legacy store so an earlier run cannot leak into this one.
try {
    localStorage.clear();
} catch { /* no storage location */ }
if (peerState?.initPeerState) await peerState.initPeerState({ filePath: stateFile, legacy: undefined });

function fileName(i: number): string {
    const folder = `フォルダ_${String(Math.floor(i / 1000)).padStart(3, "0")}`;
    return `${folder}/日本語の長いファイル名のノート_議事録と報告書の下書きメモ_${String(i).padStart(6, "0")}.md`;
}

async function makeVault() {
    await Deno.mkdir(vault, { recursive: true });
    const started = performance.now();
    for (let i = 0; i < count; i += 1) {
        const path = join(vault, fileName(i));
        if (i % 1000 === 0) await Deno.mkdir(join(vault, fileName(i)).replace(/[\\/][^\\/]*$/, ""), { recursive: true });
        await Deno.writeTextFile(path, `note ${i}\n`);
    }
    console.log(`vault: ${count} files created in ${((performance.now() - started) / 1000).toFixed(1)} s (${vault})`);
}

async function scan(label: string) {
    const conf: PeerStorageConf = {
        type: "storage",
        name: "scan-large-vault" + "n".repeat(Number(flags["name-length"])),
        baseDir: vault + "/",
        scanOfflineChanges: true,
    };
    const delivered = new Set<string>();
    const errors: string[] = [];
    let last = performance.now();
    const dispatch = (_source: unknown, path: string, _data: FileData | false) => {
        delivered.add(path);
        last = performance.now();
        return Promise.resolve();
    };
    const peer = new PeerStorage(conf, dispatch as never);
    peer.normalLog = (message, level) => {
        if (level === LOG_LEVEL_NOTICE) errors.push(message);
    };
    peer.sendLog = () => {};
    peer.debugLog = () => {};
    const started = performance.now();
    peer.start().catch((ex) => errors.push(`start failed: ${ex}`));
    // Done when everything was delivered, or when nothing new arrives for a while.
    const quiet = label === "second scan" ? 10_000 : 60_000;
    while (performance.now() - started < timeoutMs) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        if (delivered.size >= count) break;
        if (performance.now() - last > quiet && performance.now() - started > quiet) break;
    }
    const seconds = (last - started) / 1000;
    await peer.stop();
    const noticeKinds = new Map<string, number>();
    for (const e of errors) {
        const kind = e.replace(/:.*$/, "");
        noticeKinds.set(kind, (noticeKinds.get(kind) ?? 0) + 1);
    }
    console.log(
        `${label}: sent ${delivered.size}/${count}, ` +
            `${delivered.size > 0 ? seconds.toFixed(1) : "-"} s until the last file was sent` +
            (errors.length ? `, notices: ${JSON.stringify([...noticeKinds])}` : ""),
    );
    return delivered.size;
}

try {
    if (!flags.vault) await makeVault();
    console.log(`state file: ${stateFile}`);
    await scan("first scan");
    await scan("second scan");
} finally {
    if (!flags.keep) {
        if (!flags.vault) await Deno.remove(vault, { recursive: true }).catch(() => {});
        await Deno.remove(work, { recursive: true }).catch(() => {});
    } else {
        console.log(`kept: ${work}`);
    }
    Deno.exit(0);
}
