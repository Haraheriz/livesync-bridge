import { join } from "@std/path";
import { LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";
import { PeerCouchDB } from "./PeerCouchDB.ts";
import { PeerStorage } from "./PeerStorage.ts";
import { closePeerState, defaultStateFile, errorCode, getPeerState, initPeerState, type LegacyKeyValueSource } from "./PeerState.ts";
import type { FileData, PeerCouchDBConf, PeerStorageConf } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}

function assert(condition: unknown, message: string) {
    if (!condition) throw new Error(message);
}

class FakeLegacyStorage implements LegacyKeyValueSource {
    cleared = false;
    constructor(readonly entries: Map<string, string>) {}
    get length() {
        return this.entries.size;
    }
    key(index: number) {
        return [...this.entries.keys()][index] ?? null;
    }
    getItem(key: string) {
        return this.entries.get(key) ?? null;
    }
    clear() {
        this.cleared = true;
        this.entries.clear();
    }
}

async function withState(
    fn: (ctx: { dir: string; file: string }) => Promise<void>,
) {
    const dir = await Deno.makeTempDir({ prefix: "lsb-peer-state-test-" });
    try {
        await fn({ dir, file: join(dir, "state.sqlite") });
    } finally {
        await closePeerState();
        await Deno.remove(dir, { recursive: true }).catch(() => {});
    }
}

Deno.test("defaultStateFile prefers LSB_STATE_FILE, then DENO_DIR/location_data, then dat/", () => {
    const env = (values: Record<string, string>) => (key: string) => values[key];
    assertEquals(defaultStateFile(env({ LSB_STATE_FILE: "x/s.sqlite", DENO_DIR: "/deno-dir" })), "x/s.sqlite", "explicit path wins");
    assertEquals(
        defaultStateFile(env({ DENO_DIR: "/deno-dir" })).replaceAll("\\", "/"),
        "/deno-dir/location_data/livesync-bridge-state.sqlite",
        "the Compose volume location is used when DENO_DIR is set",
    );
    assertEquals(defaultStateFile(env({})).replaceAll("\\", "/"), "dat/livesync-bridge-state.sqlite", "fallback is dat/");
});

Deno.test("peer state keeps more than 10 MB and survives reopening", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined });
        // 40,000 records of about 300 bytes: past the 10 MB localStorage limit.
        const entries: [string, string][] = [];
        for (let i = 0; i < 40_000; i++) {
            entries.push([`peer-storage-./vault/-file-stat-フォルダ/ノート_${i}.md`, `${i}-${"あ".repeat(100)}`]);
        }
        let bytes = 0;
        for (const [key, value] of entries) bytes += new TextEncoder().encode(key + value).byteLength;
        assert(bytes > 10 * 1024 * 1024, `the fixture must exceed 10 MB (was ${bytes})`);
        await Promise.all(entries.map(([key, value]) => state.setValue(key, value)));
        // A single value above the limit as well.
        const big = "x".repeat(11 * 1024 * 1024);
        await state.setValue("big", big);

        await closePeerState();
        const reopened = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await reopened.getValue(entries[0][0]), entries[0][1], "the first record must be readable");
        assertEquals(await reopened.getValue(entries[39_999][0]), entries[39_999][1], "the last record must be readable");
        assertEquals((await reopened.getValue("big"))?.length, big.length, "an 11 MB value must be readable");
        assertEquals(await reopened.getValue("missing"), null, "an absent key reads as null");
    }));

Deno.test("concurrent writes are all saved, and the last write to a key wins", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined });
        const writes: Promise<void>[] = [];
        for (let i = 0; i < 5000; i++) writes.push(state.setValue(`k${i}`, `v${i}`));
        writes.push(state.setValue("same", "first"), state.setValue("same", "second"));
        await Promise.all(writes);
        assertEquals(await state.getValue("k0"), "v0", "the first write is saved");
        assertEquals(await state.getValue("k4999"), "v4999", "the last write is saved");
        assertEquals(await state.getValue("same"), "second", "writes to one key apply in order");
    }));

Deno.test("every concurrent write sees the failure when the state cannot be saved", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined });
        await state.setValue("before", "1");
        await state.dispose();
        const results = await Promise.allSettled([state.setValue("a", "1"), state.setValue("b", "2")]);
        for (const result of results) {
            if (result.status !== "rejected") throw new Error("a write that was not saved must reject");
            assertEquals((result.reason as { code?: string }).code, "unavailable", "the stable error code is passed on");
        }
    }));

Deno.test("legacy localStorage entries are copied once, without overwriting newer state", () =>
    withState(async ({ file }) => {
        const keyStat = "vault-storage-./vault/-file-stat-フォルダ/ノート.md";
        const keySince = 'couchdb-checkpoint:["remote","./vault/","https://example.invalid/couch","db","since"]';
        const legacy = new FakeLegacyStorage(new Map([[keyStat, "1740000000000-123"], [keySince, "40-old"], ["empty", ""]]));
        // A newer value written before the import must win.
        const first = await initPeerState({ filePath: file, legacy: undefined });
        await first.setValue(keySince, "41-newer");
        await closePeerState();

        const state = await initPeerState({ filePath: file, legacy });
        assertEquals(await state.getValue(keyStat), "1740000000000-123", "a legacy record is imported");
        assertEquals(await state.getValue(keySince), "41-newer", "existing state takes precedence");
        assertEquals(await state.getValue("empty"), "", "an empty string is data");
        assertEquals(legacy.entries.size, 3, "the legacy source is left untouched");
        assert(!legacy.cleared, "the legacy source must not be cleared by an import");

        // Entries added to the legacy source later are not imported: the import runs once.
        legacy.entries.set("added-later", "1");
        await closePeerState();
        const again = await initPeerState({ filePath: file, legacy });
        assertEquals(await again.getValue("added-later"), null, "the completion marker survives reopening");
        assertEquals(await again.getValue(keyStat), "1740000000000-123", "imported records stay");
    }));

Deno.test("--reset clears the state and the legacy source and is not undone by a later import", () =>
    withState(async ({ file }) => {
        const legacy = new FakeLegacyStorage(new Map([["old", "1"]]));
        const state = await initPeerState({ filePath: file, legacy });
        await state.setValue("new", "2");
        assertEquals(await state.getValue("old"), "1", "the legacy entry is imported first");

        const retained = new FakeLegacyStorage(new Map([["old", "1"]]));
        await closePeerState();
        const cleared = await initPeerState({ filePath: file, legacy: retained, reset: true });
        assertEquals(await cleared.getValue("old"), null, "reset removes imported records");
        assertEquals(await cleared.getValue("new"), null, "reset removes new records");
        assert(retained.cleared, "reset clears the legacy source");

        // Even a legacy source that still holds data must not be imported after a reset.
        await closePeerState();
        const later = await initPeerState({ filePath: file, legacy: new FakeLegacyStorage(new Map([["old", "1"]])) });
        assertEquals(await later.getValue("old"), null, "reset must not resurrect legacy state");
    }));

Deno.test("getPeerState fails clearly before initialisation", async () => {
    await closePeerState();
    let message = "";
    try {
        getPeerState();
    } catch (ex) {
        message = `${ex}`;
    }
    assert(message.includes("initPeerState"), "the error must say what to call");
});

function makeCouchPeer() {
    const config: PeerCouchDBConf = {
        type: "couchdb",
        name: "state-peer",
        baseDir: "",
        url: "https://one.example.test/proxy",
        database: "notes",
        username: "",
        password: "",
        passphrase: "",
        obfuscatePassphrase: "",
    };
    const peer = new PeerCouchDB(config, () => Promise.resolve());
    (peer as unknown as { man: { since: string } }).man = { since: "now" };
    return peer;
}

Deno.test("advanceSince persists to the real state file and survives reopening", () =>
    withState(async ({ file }) => {
        await initPeerState({ filePath: file, legacy: undefined });
        const peer = makeCouchPeer();
        await peer.advanceSince("42-token");
        assertEquals(peer.man.since, "42-token", "the reconnect position follows a saved checkpoint");
        await closePeerState();
        await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await makeCouchPeer().getSetting("since"), "42-token", "the position is still there after a restart");
    }));

Deno.test("advanceSince keeps the previous position when the real state cannot be saved", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined });
        const peer = makeCouchPeer();
        await peer.advanceSince("41-token");
        // A disposed store rejects every later write with a ValueStoreError.
        await state.dispose();
        let code: unknown;
        try {
            await peer.advanceSince("43-unsaved");
        } catch (ex) {
            code = (ex as { code?: unknown }).code;
        }
        assertEquals(code, "unavailable", "the save failure must reach the checkpoint callback with its code");
        assertEquals(peer.man.since, "41-token", "a failed save must not advance the reconnect position");
    }));

// The reproduction of the original failure: a scan whose per-file records exceed
// 10 MB. A long peer name makes each record large, so a few thousand files are
// enough to pass the limit that 70,000 ordinary files reach.
Deno.test("an offline scan sends every file even when its records exceed 10 MB", () =>
    withState(async ({ dir, file }) => {
        await initPeerState({ filePath: file, legacy: undefined });
        const vault = join(dir, "vault");
        const count = 3000;
        for (let folder = 0; folder < count / 500; folder++) await Deno.mkdir(join(vault, `フォルダ_${folder}`), { recursive: true });
        for (let i = 0; i < count; i++) {
            await Deno.writeTextFile(join(vault, `フォルダ_${Math.floor(i / 500)}`, `ノート_${i}.md`), `note ${i}`);
        }
        const name = `large-state-${"n".repeat(4000)}`;
        const conf: PeerStorageConf = { type: "storage", name, baseDir: vault + "/", scanOfflineChanges: true };
        const scan = async (expectNothing = false) => {
            const delivered = new Set<string>();
            const peer = new PeerStorage(conf, (_source, path) => {
                delivered.add(path);
                return Promise.resolve();
            });
            const notices: string[] = [];
            peer.normalLog = (message, level) => {
                if (level === LOG_LEVEL_NOTICE) notices.push(message);
            };
            peer.sendLog = () => {};
            peer.debugLog = () => {};
            void peer.start();
            const deadline = Date.now() + 60_000;
            let lastSize = -1;
            let stable = 0;
            while (Date.now() < deadline && stable < 6) {
                await new Promise((resolve) => setTimeout(resolve, 500));
                stable = delivered.size === lastSize && (delivered.size > 0 || expectNothing) ? stable + 1 : 0;
                lastSize = delivered.size;
                if (delivered.size >= count) break;
            }
            await peer.stop();
            return { delivered, notices };
        };
        const first = await scan();
        assertEquals(first.delivered.size, count, `every file must be sent (notices: ${first.notices.slice(0, 3).join(" | ")})`);
        assertEquals(first.notices.length, 0, "no record may fail to save");
        // The records really do exceed the localStorage limit.
        assert(count * (name.length + 60) > 10 * 1024 * 1024, "the fixture must exceed 10 MB");
        const second = await scan(true);
        assertEquals(second.delivered.size, 0, "a rescan with saved records must send nothing");
    }));

async function scanOnce(conf: PeerStorageConf, expected: number) {
    const delivered = new Set<string>();
    const peer = new PeerStorage(conf, (_source, path, _data: FileData | false) => {
        delivered.add(path);
        return Promise.resolve();
    });
    peer.sendLog = () => {};
    peer.debugLog = () => {};
    peer.normalLog = () => {};
    void peer.start();
    const deadline = Date.now() + 15_000;
    while (delivered.size < expected && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 50));
    // Rescans send nothing: give them a moment to prove it.
    if (expected === 0) await new Promise((resolve) => setTimeout(resolve, 1500));
    await peer.stop();
    return { delivered, peer };
}

Deno.test("a failure to save file records is logged once per batch and the files are still sent", () =>
    withState(async ({ dir, file }) => {
        const failures: { code: string; count: number }[] = [];
        const state = await initPeerState({
            filePath: file,
            legacy: undefined,
            queue: { onDeferredError: (error, count) => failures.push({ code: errorCode(error), count }) },
        });
        (state as unknown as { state: { writeBatch: () => Promise<void> } }).state.writeBatch = () =>
            Promise.reject(Object.assign(new Error("secret detail"), { code: "quota-exceeded" }));
        const vault = join(dir, "vault");
        await Deno.mkdir(vault);
        for (let i = 0; i < 20; i++) await Deno.writeTextFile(join(vault, `n${i}.md`), `note ${i}`);
        const { delivered } = await scanOnce({ type: "storage", name: "save-fail", baseDir: vault + "/", scanOfflineChanges: true }, 20);
        assertEquals(delivered.size, 20, "every file must be sent although its record could not be saved");
        await state.flush();
        assert(failures.length >= 1 && failures.length <= 3, `failures are reported per batch, not per file (${failures.length})`);
        assertEquals(failures.reduce((sum, f) => sum + f.count, 0), 20, "every lost record is counted");
        assertEquals(failures[0].code, "quota-exceeded", "only the stable code is reported");
    }));

Deno.test("a queued record is readable before it is saved, and flush() saves it", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined, queue: { delayMs: 3_600_000, batchSize: 1000 } });
        state.setValueLater("k", "queued");
        assertEquals(await state.getValue("k"), "queued", "a read sees the queued value");
        state.setValueLater("k", "newer");
        assertEquals(await state.getValue("k"), "newer", "the newest queued value wins");
        await state.flush();
        assertEquals(await state.getValue("k"), "newer", "the value is still there after the flush");
        await closePeerState();
        const reopened = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await reopened.getValue("k"), "newer", "and it was saved to the file");
    }));

Deno.test("a read during a commit sees the value that is being committed", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined, queue: { delayMs: 3_600_000 } });
        const inner = (state as unknown as { state: { writeBatch: (ops: unknown[]) => Promise<void> } }).state;
        const original = inner.writeBatch.bind(inner);
        let release!: () => void;
        const gate = new Promise<void>((resolve) => (release = resolve));
        inner.writeBatch = async (ops) => {
            await gate;
            return original(ops);
        };
        state.setValueLater("k", "v1");
        const flushing = state.flush();
        await new Promise((resolve) => setTimeout(resolve, 20));
        assertEquals(await state.getValue("k"), "v1", "the commit has not finished, the queue still answers");
        release();
        await flushing;
        assertEquals(await state.getValue("k"), "v1", "and the store answers once it has");
    }));

Deno.test("dispose() saves queued records", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined, queue: { delayMs: 3_600_000 } });
        for (let i = 0; i < 2500; i++) state.setValueLater(`k${i}`, `${i}`);
        await closePeerState();
        const reopened = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await reopened.getValue("k0"), "0", "the first record was saved");
        assertEquals(await reopened.getValue("k2499"), "2499", "the last record was saved");
    }));

Deno.test("a record queued by time is saved without flush()", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined, queue: { delayMs: 30 } });
        state.setValueLater("k", "v");
        await new Promise((resolve) => setTimeout(resolve, 300));
        const inner = (state as unknown as { queued: Map<string, string> }).queued;
        assertEquals(inner.size, 0, "the queue was emptied by the timer");
        await closePeerState();
        assertEquals(await (await initPeerState({ filePath: file, legacy: undefined })).getValue("k"), "v", "and the file has the record");
    }));

Deno.test("advanceSince does not move the position when the batch that carries it fails", () =>
    withState(async ({ file }) => {
        const state = await initPeerState({ filePath: file, legacy: undefined, queue: { onDeferredError: () => {} } });
        const peer = makeCouchPeer();
        await peer.advanceSince("41-token");
        state.setValueLater("queued-before", "x");
        (state as unknown as { state: { writeBatch: () => Promise<void> } }).state.writeBatch = () =>
            Promise.reject(Object.assign(new Error("disk"), { code: "operation-failed" }));
        let code: unknown;
        try {
            await peer.advanceSince("43-unsaved");
        } catch (ex) {
            code = (ex as { code?: unknown }).code;
        }
        assertEquals(code, "operation-failed", "the failure reaches the caller");
        assertEquals(peer.man.since, "41-token", "the reconnect position stays where it was");
    }));

async function crashAndRescan(mode: "none" | "late", dir: string, file: string) {
    const vault = join(dir, "vault");
    await Deno.mkdir(vault);
    const total = 25;
    for (let i = 0; i < total; i++) await Deno.writeTextFile(join(vault, `n${i}.md`), `note ${i}`);
    const child = await new Deno.Command(Deno.execPath(), {
        args: ["run", "-A", "test/peerStateCrashChild.ts", vault, file, mode, `${total}`],
        stdout: "piped",
        stderr: "piped",
    }).output();
    assertEquals(child.code, 0, `the child must finish: ${new TextDecoder().decode(child.stderr)}`);
    await initPeerState({ filePath: file, legacy: undefined });
    const conf: PeerStorageConf = { type: "storage", name: "crash", baseDir: vault + "/", scanOfflineChanges: true };
    const expectedResent = mode === "none" ? total : 5;
    const { delivered } = await scanOnce(conf, expectedResent);
    // Anything beyond the expected files would show up in the second pass.
    const again = await scanOnce(conf, 0);
    return { total, delivered, again: again.delivered };
}

Deno.test("when no record was saved before a crash, the next scan sends every file", () =>
    withState(async ({ dir, file }) => {
        const { total, delivered, again } = await crashAndRescan("none", dir, file);
        assertEquals(delivered.size, total, "no file may be missed");
        assertEquals(again.size, 0, "and the records written by that scan are complete");
    }));

Deno.test("records lost in a crash are re-sent, and records that were saved are not", () =>
    withState(async ({ dir, file }) => {
        const { delivered, again } = await crashAndRescan("late", dir, file);
        assertEquals([...delivered].sort().join(","), "n0.md,n1.md,n2.md,n3.md,n4.md", "only the five edited files lost their records");
        assertEquals(again.size, 0, "a further scan sends nothing");
    }));
