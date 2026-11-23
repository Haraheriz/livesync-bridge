// excludeNames / excludeDotNames: names left out of synchronisation in both directions.
// Sending side: start-up scan, live watcher (Deno.watchFs and Chokidar), periodic and new-folder
// rescans. Receiving side: writes, deletions and moves arriving from other peers.
import chokidar from "chokidar";
import { join } from "@std/path";
import { DEFAULT_EXCLUDE_NAMES, PeerStorage } from "./PeerStorage.ts";
import { Hub, internalFileExclusionNotices } from "./Hub.ts";
import type { Peer } from "./Peer.ts";
import type { Config, FileData, PeerConf, PeerStorageConf } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function waitFor(condition: () => boolean, message: string, timeoutMs = 10_000) {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error(`Timed out while waiting: ${message}`);
        await sleep(10);
    }
}
async function assertRejects(run: () => unknown, pattern: RegExp, message: string) {
    try {
        await run();
    } catch (error) {
        if (!pattern.test((error as Error).message)) {
            throw new Error(`${message}: wrong error: ${(error as Error).message}`);
        }
        return;
    }
    throw new Error(`${message}: nothing was thrown`);
}

function text(s: string): FileData {
    return { ctime: 1_700_000_000_000, mtime: 1_700_000_000_000, size: s.length, data: [s] };
}
async function exists(path: string) {
    return await Deno.stat(path).then(() => true, () => false);
}
async function write(dir: string, path: string, content = "x") {
    await Deno.mkdir(join(dir, path, ".."), { recursive: true });
    await Deno.writeTextFile(join(dir, path), content);
}

type Handler = (...args: unknown[]) => unknown;
class FakeWatcher {
    private handlers = new Map<string, Handler[]>();
    on(event: string, handler: Handler) {
        this.handlers.set(event, [...(this.handlers.get(event) ?? []), handler]);
        return this;
    }
    emit(event: string, ...args: unknown[]) {
        for (const handler of this.handlers.get(event) ?? []) handler(...args);
    }
    close() {
        return Promise.resolve();
    }
}

// A Deno.watchFs stand-in which reports nothing until it is closed.
class QuietDenoWatcher implements AsyncIterator<Deno.FsEvent>, AsyncIterable<Deno.FsEvent> {
    private result = Promise.withResolvers<IteratorResult<Deno.FsEvent>>();
    next() {
        return this.result.promise;
    }
    [Symbol.asyncIterator]() {
        return this;
    }
    close() {
        this.result.resolve({ done: true, value: undefined });
    }
    [Symbol.dispose]() {
        this.close();
    }
}

// File records are kept in memory (the real queue defers saving by a timer). `sent` lists "+path" for
// a send and "-path" for a deletion; `moves` holds the previous path of every send that had one.
function makePeer(baseDir: string, extra: Partial<PeerStorageConf> = {}) {
    const config: PeerStorageConf = {
        type: "storage",
        name: `exclude-${crypto.randomUUID()}`,
        baseDir,
        scanOfflineChanges: false,
        useChokidar: false,
        rescanIntervalMinutes: 0,
        ...extra,
    };
    const peer = new PeerStorage(config, () => Promise.resolve());
    const settings = new Map<string, string>();
    peer.setSetting = (key, value) => {
        settings.set(key, value);
        return Promise.resolve();
    };
    peer.getSetting = (key) => Promise.resolve(settings.get(key) ?? null);
    peer.setSettingLater = (key, value) => void peer.setSetting(key, value);
    const notices: string[] = [];
    peer.normalLog = (message) => void notices.push(message);
    peer.debugLog = () => {};
    peer.sendLog = () => {};
    peer.receiveLog = () => {};
    const sent: string[] = [];
    const moves: string[] = [];
    peer.dispatchToHub = (_source, path, data, context) => {
        sent.push(`${data === false ? "-" : "+"}${path}`);
        if (context?.previousPath !== undefined) moves.push(`${context.previousPath}>${path}`);
        return Promise.resolve();
    };
    // Every directory the peer lists.
    const listed: string[] = [];
    peer.readDirectory = (path) => {
        listed.push(path.replaceAll("\\", "/"));
        return Deno.readDir(path);
    };
    const plus = () => sent.filter((entry) => entry.startsWith("+")).sort().join(",");
    const minus = () => sent.filter((entry) => entry.startsWith("-")).sort().join(",");
    return { peer, sent, moves, notices, listed, plus, minus };
}

async function withDir(run: (dir: string) => Promise<void>) {
    const dir = await Deno.makeTempDir({ prefix: "exclude-" });
    try {
        await run(dir);
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
}

// Both watcher modes: Deno.watchFs (the default) and Chokidar.
const MODES = [{ useChokidar: false, label: "Deno.watchFs" }, { useChokidar: true, label: "Chokidar" }];

// Records every path handed to Deno.stat, Deno.lstat and Deno.realPath while `run` is going,
// to show that excluded paths are not even looked at.
async function recordingFileCalls(run: () => Promise<void>): Promise<string[]> {
    const calls: string[] = [];
    const originals = { stat: Deno.stat, lstat: Deno.lstat, realPath: Deno.realPath };
    const wrap = <T extends (path: string | URL) => Promise<unknown>>(original: T): T =>
        ((path: string | URL) => {
            calls.push(String(path).replaceAll("\\", "/"));
            return original(path);
        }) as T;
    Deno.stat = wrap(originals.stat);
    Deno.lstat = wrap(originals.lstat);
    Deno.realPath = wrap(originals.realPath);
    try {
        await run();
    } finally {
        Deno.stat = originals.stat;
        Deno.lstat = originals.lstat;
        Deno.realPath = originals.realPath;
    }
    return calls;
}
const touchesExcluded = (calls: string[]) => calls.filter((path) => /@eaDir|\.sync-canary|\.trash|#recycle/.test(path));

function isReady(peer: PeerStorage) {
    // deno-lint-ignore no-explicit-any
    const internal = peer as any;
    return peer.health().ok || internal.watcherReady === true;
}

// ---- isExcludedPath ----

Deno.test("isExcludedPath: Synology folders and dot names at any depth, by default", () => {
    const { peer } = makePeer("/tmp");
    const cases: [string, boolean][] = [
        ["note.md", false],
        ["foursquare/2026-10.md", false],
        ["@eaDir/x.jpg", true],
        ["A/@eaDir/photo.jpg@SynoEAStream", true],
        ["A/B/@eaDir", true],
        ["#recycle/old.md", true],
        ["#snapshot/GMT+09-2026.10.18-01.15.00/note.md", true],
        [".obsidian/workspace.json", true],
        [".sync-canary", true],
        ["A/.trash/x.md", true],
        ["ex/.s-hidden.md", true],
        ["#tag-like name.md", false], // only the exact names are excluded
        ["@mention.md", false],
        ["@eaDir.md", false],
        ["a@eaDir/x.md", false],
        ["A/B/c.md", false],
        ["A/..b/c.md", true], // a dot name, whatever follows the dot
        ["./@eaDir/x.md", true],
        ["A//@eaDir/x.md", true],
        ["@eaDir/../x.md", true],
        ["", false],
        [".", false],
        ["..", false],
    ];
    for (const [path, expected] of cases) {
        assertEquals(peer.isExcludedPath(path), expected, `isExcludedPath(${JSON.stringify(path)})`);
    }
    assertEquals(DEFAULT_EXCLUDE_NAMES.join(","), "@eaDir,#recycle,#snapshot", "the Synology names are the defaults");
});

Deno.test("isExcludedPath: absent keys mean the defaults, and both can be configured", () => {
    const { peer: bare } = makePeer("/tmp");
    assertEquals(bare.config.excludeNames, undefined, "the key is absent");
    assertEquals(bare.isExcludedPath("@eaDir/x"), true, "default names apply without the key");
    assertEquals(bare.isExcludedPath(".git/x"), true, "default dot rule applies without the key");

    const { peer } = makePeer("/tmp", { excludeNames: ["skip"], excludeDotNames: false });
    assertEquals(peer.isExcludedPath("A/skip/x.md"), true, "a configured name");
    assertEquals(peer.isExcludedPath("@eaDir/x.jpg"), false, "the defaults are replaced, not added to");
    assertEquals(peer.isExcludedPath(".obsidian/app.json"), false, "dot rule off");

    const { peer: dotsOnly } = makePeer("/tmp", { excludeNames: [] });
    assertEquals(dotsOnly.isExcludedPath("@eaDir/x.jpg"), false, "an empty list excludes no name");
    assertEquals(dotsOnly.isExcludedPath(".obsidian/app.json"), true, "the dot rule stays on");

    const { peer: namesOnly } = makePeer("/tmp", { excludeDotNames: false });
    assertEquals(namesOnly.isExcludedPath("#recycle/x.md"), true, "default names with the dot rule off");
    assertEquals(namesOnly.isExcludedPath("A/.git/x"), false, "no dot rule");

    const { peer: dotName } = makePeer("/tmp", { excludeNames: [".keep"], excludeDotNames: false });
    assertEquals(dotName.isExcludedPath("A/.keep/x"), true, "a dot name listed explicitly");
    assertEquals(dotName.isExcludedPath("A/.other/x"), false, "other dot names stay");
});

Deno.test("isExcludedPath: with pathNormalizationVersion 1 names are compared as the peer compares paths", () => {
    const nfc = "caf\u00e9";
    const nfd = "cafe\u0301";
    const { peer: legacy } = makePeer("/tmp", { excludeNames: [nfc] });
    assertEquals(legacy.isExcludedPath(`${nfc}/x`), true, "legacy: the same spelling");
    assertEquals(legacy.isExcludedPath(`${nfd}/x`), false, "legacy: exact comparison, another spelling is another name");
    assertEquals(legacy.isExcludedPath("@EADIR/x"), false, "legacy: case-sensitive");

    const { peer } = makePeer("/tmp", { pathNormalizationVersion: 1, excludeNames: [nfc, "#Recycle"] });
    assertEquals(peer.isExcludedPath(`${nfd}/x`), true, "version 1: NFD spelling of an NFC name");
    assertEquals(peer.isExcludedPath("a/#RECYCLE/x"), true, "version 1: case-folded unless case sensitive");
    assertEquals(peer.isExcludedPath("@EADIR/x"), false, "version 1: the names were replaced");

    const { peer: sensitive } = makePeer("/tmp", {
        pathNormalizationVersion: 1,
        handleFilenameCaseSensitive: true,
        excludeNames: ["#Recycle"],
    });
    assertEquals(sensitive.isExcludedPath("#RECYCLE/x"), false, "version 1, case sensitive: another name");
    assertEquals(sensitive.isExcludedPath("#Recycle/x"), true, "version 1, case sensitive: the same name");
});

Deno.test("a misconfigured excludeNames or excludeDotNames is refused instead of excluding too much or too little", async () => {
    const make = (extra: Record<string, unknown>) => () =>
        new PeerStorage({ type: "storage", name: "bad", baseDir: "/tmp", ...extra } as PeerStorageConf, () => Promise.resolve());
    await assertRejects(make({ excludeNames: "@eaDir" }), /array of file or folder names/, "a string");
    await assertRejects(make({ excludeNames: [1] }), /array of file or folder names/, "a number");
    await assertRejects(make({ excludeNames: ["a/b"] }), /cannot contain "\/"/, "a path");
    await assertRejects(make({ excludeDotNames: "no" }), /true or false/, "a string flag");
    make({ excludeNames: [], excludeDotNames: false })(); // valid
});

// ---- Sending side: start-up scan and the live watcher, in both watcher modes ----

for (const mode of MODES) {
    Deno.test(`[${mode.label}] the start-up scan sends vault files but nothing under excluded names`, async () => {
        await withDir(async (dir) => {
            for (const file of [
                "@eaDir/thumb.jpg", "A/@eaDir/thumb.jpg", "A/@eaDir/deep/more/thumb.jpg", ".obsidian/app.json",
                "#recycle/old.md", "#snapshot/GMT/note.md", ".sync-canary", "A/.trash/x.md", "ex/.s-hidden.md",
            ]) await write(dir, file);
            await write(dir, "note.md", "n");
            await write(dir, "A/a.md", "a");
            await write(dir, "@mention.md", "m");
            const { peer, plus, minus, listed } = makePeer(dir, { scanOfflineChanges: true, useChokidar: mode.useChokidar });
            let starting: Promise<void> | undefined;
            try {
                starting = peer.start();
                await waitFor(() => plus() === "+@mention.md,+A/a.md,+note.md", "vault files are sent");
                await sleep(800); // anything excluded would have been sent by now
                assertEquals(plus(), "+@mention.md,+A/a.md,+note.md", "only the vault files are sent");
                assertEquals(minus(), "", "no deletion");
                if (!mode.useChokidar) {
                    const entered = listed.filter((path) => /(^|\/)(@eaDir|#recycle|#snapshot|\.obsidian|\.trash)(\/|$)/.test(path));
                    assertEquals(entered.join(","), "", "excluded folders are not even listed");
                }
            } finally {
                await peer.stop();
                await starting;
            }
        });
    });

    Deno.test(`[${mode.label}] files created and deleted later under excluded names are not sent; others are`, async () => {
        await withDir(async (dir) => {
            await write(dir, "@eaDir/old.jpg");
            await write(dir, ".sync-canary");
            await write(dir, "del.md", "d");
            const { peer, plus, minus } = makePeer(dir, { useChokidar: mode.useChokidar });
            let starting: Promise<void> | undefined;
            try {
                starting = peer.start();
                await waitFor(() => isReady(peer), "the watcher is ready");
                await sleep(1_000);
                await Deno.remove(join(dir, "@eaDir/old.jpg"));
                await Deno.remove(join(dir, ".sync-canary"));
                await write(dir, "B/@eaDir/thumb.jpg", "t");
                await write(dir, "#recycle/gone.md", "g");
                await write(dir, ".obsidian/workspace.json", "{}");
                await write(dir, "B/.trash/x.md", "x");
                await Deno.remove(join(dir, "del.md"));
                await write(dir, "B/b.md", "b");
                await waitFor(() => plus() === "+B/b.md" && minus() === "-del.md", "the normal changes are sent");
                await sleep(1_500); // anything excluded would have been sent by now
                assertEquals(plus(), "+B/b.md", "only B/b.md is sent");
                assertEquals(minus(), "-del.md", "only the deletion of a normal file is sent");
            } finally {
                await peer.stop();
                await starting;
            }
        });
    });
}

// ---- Rescans ----

for (const mode of MODES) {
    Deno.test(`[${mode.label}] a rescan does not send files under excluded names, and does not descend into them`, async () => {
        await withDir(async (dir) => {
            await write(dir, "C/@eaDir/thumb.jpg");
            await write(dir, "C/@eaDir/deep/thumb.jpg");
            await write(dir, "C/.hidden/h.md");
            await write(dir, "#recycle/x.md");
            await write(dir, ".sync-canary");
            await write(dir, "C/c.md", "c");
            const { peer, plus, listed } = makePeer(dir, { useChokidar: mode.useChokidar });
            try {
                assertEquals(await peer.rescan(dir, "test"), 1, "one file is found");
                await waitFor(() => plus() === "+C/c.md", "C/c.md is sent");
                await sleep(500);
                assertEquals(plus(), "+C/c.md", "only C/c.md is sent");
                const entered = listed.filter((path) => /(^|\/)(@eaDir|#recycle|\.hidden)(\/|$)/.test(path));
                assertEquals(entered.join(","), "", "excluded folders are not listed");
                // A rescan asked to look inside an excluded folder does nothing.
                const before = listed.length;
                assertEquals(await peer.rescan(join(dir, "C/@eaDir"), "new folder"), 0, "nothing in an excluded folder");
                assertEquals(listed.length, before, "and it is not listed");
            } finally {
                await peer.stop();
            }
        });
    });
}

async function withFakeChokidar(
    extra: Partial<PeerStorageConf>,
    body: (ctx: {
        dir: string;
        peer: PeerStorage;
        plus: () => string;
        minus: () => string;
        watcher: FakeWatcher;
        options: () => { ignored: (path: string) => boolean };
    }) => Promise<void>,
) {
    const original = chokidar.watch;
    const dir = await Deno.makeTempDir({ prefix: "exclude-fake-" });
    const watcher = new FakeWatcher();
    let options: { ignored: (path: string) => boolean } | undefined;
    chokidar.watch = ((_path: unknown, given: { ignored: (path: string) => boolean }) => {
        options = given;
        return watcher as unknown as ReturnType<typeof chokidar.watch>;
    }) as unknown as typeof chokidar.watch;
    const { peer, plus, minus } = makePeer(dir, { useChokidar: true, ...extra });
    try {
        await peer.start();
        await body({ dir, peer, plus, minus, watcher, options: () => options! });
    } finally {
        chokidar.watch = original;
        await peer.stop();
        await Deno.remove(dir, { recursive: true });
    }
}

async function withQuietDenoWatcher(
    extra: Partial<PeerStorageConf>,
    body: (ctx: {
        dir: string;
        peer: PeerStorage;
        plus: () => string;
        minus: () => string;
        sent: string[];
        moves: string[];
        listed: string[];
    }) => Promise<void>,
    prepare: (dir: string) => Promise<void> = () => Promise.resolve(),
) {
    const original = Deno.watchFs;
    const dir = await Deno.makeTempDir({ prefix: "exclude-deno-" });
    Deno.watchFs = (() => new QuietDenoWatcher() as unknown as Deno.FsWatcher) as typeof Deno.watchFs;
    const made = makePeer(dir, { useChokidar: false, ...extra });
    let starting: Promise<void> | undefined;
    try {
        await prepare(dir);
        starting = made.peer.start();
        await waitFor(() => made.peer.health().ok, "the Deno watcher is ready");
        await body({ dir, ...made });
    } finally {
        await made.peer.stop();
        await starting;
        Deno.watchFs = original;
        await Deno.remove(dir, { recursive: true });
    }
}

Deno.test("[Chokidar] a new excluded folder is not rescanned; a new normal folder is, without its excluded subfolders", async () => {
    await withFakeChokidar({}, async ({ dir, peer, plus, watcher }) => {
        watcher.emit("ready");
        await write(dir, "@eaDir/t.jpg");
        await write(dir, ".hidden/h.md");
        await write(dir, "N/@eaDir/t.jpg");
        await write(dir, "N/n.md", "n");
        watcher.emit("addDir", join(dir, "@eaDir"));
        watcher.emit("addDir", join(dir, ".hidden"));
        watcher.emit("addDir", join(dir, "N/@eaDir"));
        // deno-lint-ignore no-explicit-any
        assertEquals((peer as any).newDirTimers.size, 0, "no rescan is even scheduled for the excluded folders");
        watcher.emit("addDir", join(dir, "N"));
        await waitFor(() => plus() === "+N/n.md", "the normal new folder is rescanned");
        await sleep(2_800); // the delay of a new-folder rescan is 2 s
        assertEquals(plus(), "+N/n.md", "nothing from the excluded folders");
    });
});

Deno.test("[Chokidar] the watcher is told to skip excluded paths, and its add, change and unlink events for them do nothing", async () => {
    await withFakeChokidar({}, async ({ dir, peer, plus, minus, watcher, options }) => {
        watcher.emit("ready");
        const ignored = options().ignored;
        assertEquals(ignored(dir), false, "the vault folder itself is watched");
        assertEquals(ignored(join(dir, "A/b.md")), false, "a normal path is watched");
        assertEquals(ignored(join(dir, "@eaDir")), true, "@eaDir is not watched");
        assertEquals(ignored(join(dir, "A/@eaDir/x.jpg")), true, "nor below another folder");
        assertEquals(ignored(join(dir, ".obsidian")), true, "a dot folder is not watched");
        await write(dir, "@eaDir/x.jpg");
        await write(dir, ".sync-canary");
        await write(dir, "ok.md", "ok");
        const records: string[] = [];
        const getSetting = peer.getSetting.bind(peer);
        peer.getSetting = (key) => (records.push(key), getSetting(key));
        const calls = await recordingFileCalls(async () => {
            for (const event of ["add", "change"]) {
                watcher.emit(event, join(dir, "@eaDir/x.jpg"));
                watcher.emit(event, join(dir, ".sync-canary"));
            }
            watcher.emit("unlink", join(dir, "@eaDir/gone.jpg"));
            watcher.emit("unlink", join(dir, "A/.trash/gone.md"));
            await sleep(100);
        });
        assertEquals(touchesExcluded(calls).join(","), "", "the events for excluded paths do not even stat them");
        assertEquals(records.length, 0, "nor look up their file records");
        await peer.dispatch(join(dir, "#recycle/x.md"));
        await peer.dispatchDeleted(join(dir, "#recycle/x.md"));
        // Positive controls: the same events for normal names are handled.
        watcher.emit("add", join(dir, "ok.md"));
        watcher.emit("unlink", join(dir, "gone.md"));
        await waitFor(() => plus() === "+ok.md" && minus() === "-gone.md", "the normal events are handled");
        await sleep(800);
        assertEquals(plus(), "+ok.md", "nothing excluded is sent");
        assertEquals(minus(), "-gone.md", "no deletion of an excluded path is sent");
    });
});

Deno.test("[Chokidar] the periodic rescan does not send excluded files", async () => {
    // 0.002 min = 120 ms, randomised to 90..150 ms
    await withFakeChokidar({ rescanIntervalMinutes: 0.002 }, async ({ dir, plus, watcher }) => {
        await write(dir, "@eaDir/t.jpg");
        await write(dir, "A/.hidden/h.md");
        await write(dir, "missed.md", "m");
        watcher.emit("ready");
        await waitFor(() => plus() === "+missed.md", "the missed file is sent");
        await sleep(1_000); // several more rescans
        assertEquals(plus(), "+missed.md", "only the vault file, once");
    });
});

Deno.test("[Deno.watchFs] the periodic rescan does not send excluded files", async () => {
    await withQuietDenoWatcher({ rescanIntervalMinutes: 0.002 }, async ({ dir, plus, listed }) => {
        await write(dir, "@eaDir/t.jpg");
        await write(dir, "A/.hidden/h.md");
        await write(dir, "#snapshot/s.md");
        await write(dir, "missed.md", "m");
        await waitFor(() => plus() === "+missed.md", "the missed file is sent");
        await sleep(1_000);
        assertEquals(plus(), "+missed.md", "only the vault file, once");
        assertEquals(listed.filter((path) => /@eaDir|\.hidden|#snapshot/.test(path)).length, 0, "excluded folders are not listed");
    });
});

Deno.test("[Deno.watchFs] watcher events for excluded paths are dropped without being looked at", async () => {
    await withQuietDenoWatcher({}, async ({ dir, peer, plus, minus }) => {
        await write(dir, "@eaDir/x.jpg");
        await write(dir, "A/.trash/x.md");
        await write(dir, "#recycle/gone.md");
        await write(dir, "ok.md", "ok");
        const calls = await recordingFileCalls(async () => {
            peer.processFile({ kind: "create", paths: [join(dir, "@eaDir")] });
            peer.processFile({ kind: "modify", paths: [join(dir, "A/.trash/x.md")] });
            peer.processFile({ kind: "remove", paths: [join(dir, "#recycle/gone.md")] });
            peer.processFile({ kind: "remove", paths: [join(dir, "@eaDir/old.jpg")] });
            peer.processFile({ kind: "create", paths: [join(dir, "ok.md")] });
            await waitFor(() => plus() === "+ok.md", "the normal event is handled");
            await sleep(500);
        });
        assertEquals(touchesExcluded(calls).join(","), "", "excluded paths are not even stat()ed");
        await sleep(800);
        assertEquals(plus(), "+ok.md", "nothing excluded is sent");
        assertEquals(minus(), "", "no deletion is sent");
        await peer.dispatch(join(dir, "@eaDir/x.jpg")); // also when asked directly
        await peer.dispatchDeleted(join(dir, "@eaDir/x.jpg"));
        await sleep(500);
        assertEquals(plus(), "+ok.md", "still nothing excluded");
        assertEquals(minus(), "", "still no deletion");
    });
});

Deno.test("[Deno.watchFs] on a case-insensitive filesystem an event spelled differently does not get into an excluded folder", async () => {
    await withQuietDenoWatcher({}, async ({ dir, peer, plus, listed }) => {
        if (!await exists(join(dir, "@EADIR"))) return; // a case-sensitive filesystem: there is no such alias
        peer.processFile({ kind: "create", paths: [join(dir, "@EADIR")] });
        peer.processFile({ kind: "create", paths: [join(dir, "ok.md")] });
        await waitFor(() => plus() === "+ok.md", "the normal event is handled");
        await sleep(600);
        assertEquals(plus(), "+ok.md", "nothing from the excluded folder");
        assertEquals(listed.filter((path) => /@eaDir/i.test(path)).length, 0, "and it is not listed");
    }, async (dir) => {
        await write(dir, "@eaDir/t.jpg");
        await write(dir, "ok.md", "ok");
    });
});

Deno.test("[Deno.watchFs] a move between a normal and an excluded name is a deletion or a new file, never a send of the excluded path", async () => {
    await withQuietDenoWatcher({}, async ({ dir, peer, plus, minus, moves }) => {
        // Into an excluded folder: the other peers see the file leave.
        await Deno.rename(join(dir, "m.md"), join(dir, "@eaDir/m.md"));
        peer.processFile({ kind: "rename", paths: [join(dir, "m.md"), join(dir, "@eaDir/m.md")] });
        await waitFor(() => minus() === "-m.md", "the old name is deleted");
        await sleep(600);
        assertEquals(plus(), "", "the excluded destination is not sent");
        // Out of an excluded folder: a new file, not a move.
        await Deno.rename(join(dir, "@eaDir/x.md"), join(dir, "y.md"));
        peer.processFile({ kind: "rename", paths: [join(dir, "@eaDir/x.md"), join(dir, "y.md")] });
        await waitFor(() => plus() === "+y.md", "the new name is sent");
        await sleep(600);
        assertEquals(minus(), "-m.md", "no deletion of the excluded source");
        assertEquals(moves.length, 0, "it is not announced as a move from the excluded path");
    }, async (dir) => {
        await write(dir, "m.md", "move me");
        await write(dir, "@eaDir/x.md", "from outside");
    });
});

Deno.test("[Deno.watchFs] real watcher: a folder with an excluded subtree is moved in; only the normal files are sent", async () => {
    await withDir(async (root) => {
        const vault = join(root, "vault");
        await Deno.mkdir(vault);
        await write(root, "staging/P/p.md", "p");
        await write(root, "staging/P/@eaDir/t.jpg", "t");
        await write(root, "staging/P/.hidden/h.md", "h");
        const { peer, plus } = makePeer(vault);
        let starting: Promise<void> | undefined;
        try {
            starting = peer.start();
            await waitFor(() => isReady(peer), "the watcher is ready");
            await Deno.rename(join(root, "staging/P"), join(vault, "P"));
            await waitFor(() => plus() === "+P/p.md", "the normal file is sent");
            await sleep(1_000);
            assertEquals(plus(), "+P/p.md", "only the normal file");
        } finally {
            await peer.stop();
            await starting;
        }
    });
});

Deno.test("with pathNormalizationVersion 1 the start-up scan and the rescan skip excluded folders too", async () => {
    await withQuietDenoWatcher({ pathNormalizationVersion: 1, scanOfflineChanges: true }, async ({ dir, peer, plus, listed }) => {
        await waitFor(() => plus() === "+A/a.md,+note.md", "the vault files are sent");
        await sleep(500);
        assertEquals(plus(), "+A/a.md,+note.md", "only the vault files after the start-up scan");
        assertEquals(await peer.rescan(dir, "test"), 0, "a rescan finds nothing else");
        await write(dir, "A/new.md", "n");
        await write(dir, "A/@eaDir/new.jpg");
        assertEquals(await peer.rescan(dir, "test"), 1, "a rescan sends only the new vault file");
        await waitFor(() => plus() === "+A/a.md,+A/new.md,+note.md", "the new file is sent");
        assertEquals(listed.filter((path) => /@eaDir|#recycle|\.obsidian/i.test(path)).length, 0, "excluded folders are not listed");
    }, async (dir) => {
        await write(dir, "note.md", "n");
        await write(dir, "A/a.md", "a");
        await write(dir, "@EADIR/thumb.jpg");
        await write(dir, "A/@eaDir/thumb.jpg");
        await write(dir, "#recycle/old.md");
        await write(dir, ".obsidian/app.json");
    });
});

// ---- Receiving side ----

Deno.test("changes arriving from other peers are not written or deleted under excluded names, and are not failures", async () => {
    await withDir(async (dir) => {
        const { peer, notices } = makePeer(dir);
        await write(dir, ".sync-canary", "keep");
        await write(dir, "A/@eaDir/keep.jpg", "keep");
        assertEquals(await peer.put("@eaDir/x.jpg", text("x")), false, "put into @eaDir is not applied");
        assertEquals(await peer.put("A/#recycle/x.md", text("x")), false, "put into #recycle is not applied");
        assertEquals(await peer.put(".obsidian/app.json", text("{}")), false, "put into .obsidian is not applied");
        assertEquals(await peer.put("A/.trash/x.md", text("x")), false, "put into a nested dot folder is not applied");
        assertEquals(await peer.delete(".sync-canary"), false, "delete of a dot file is not applied");
        assertEquals(await peer.delete("A/@eaDir/keep.jpg"), false, "delete under @eaDir is not applied");
        assertEquals(await peer.put("D/d.md", text("d")), true, "a normal note is written");
        assertEquals(await exists(join(dir, "@eaDir")), false, "@eaDir is not created");
        assertEquals(await exists(join(dir, "A/#recycle")), false, "#recycle is not created");
        assertEquals(await exists(join(dir, ".obsidian")), false, ".obsidian is not created");
        assertEquals(await exists(join(dir, "A/.trash")), false, ".trash is not created");
        assertEquals(await Deno.readTextFile(join(dir, ".sync-canary")), "keep", ".sync-canary is kept");
        assertEquals(await Deno.readTextFile(join(dir, "A/@eaDir/keep.jpg")), "keep", "the file in @eaDir is kept");
        assertEquals(await Deno.readTextFile(join(dir, "D/d.md")), "d", "D/d.md is written");
        assertEquals(notices.length, 0, "no notice or failure is reported");
        await peer.stop();
    });
});

Deno.test("an excluded incoming change is not remembered as handled: it is applied once the name is no longer excluded", async () => {
    await withDir(async (dir) => {
        const { peer } = makePeer(dir);
        const data = text("later");
        assertEquals(await peer.put("@eaDir/later.md", data), false, "refused while excluded");
        assertEquals(await exists(join(dir, "@eaDir")), false, "nothing is written");
        peer.config.excludeNames = [];
        assertEquals(await peer.put("@eaDir/later.md", data), true, "the same change is written once it is not excluded");
        assertEquals(await Deno.readTextFile(join(dir, "@eaDir/later.md")), "later", "with its content");
        await peer.stop();
    });
});

Deno.test("renames arriving from other peers: nothing is moved, written or deleted under an excluded name", async () => {
    await withDir(async (dir) => {
        const { peer, notices } = makePeer(dir);
        await write(dir, "old.md", "old");
        await write(dir, "@eaDir/src.md", "src");
        await write(dir, ".hidden/src.md", "src");
        // Both names excluded.
        assertEquals(await peer.rename("@eaDir/src.md", "#recycle/dst.md", text("src")), false, "excluded to excluded");
        // The new name is excluded: not stored, and the old file stays.
        assertEquals(await peer.rename("old.md", "@eaDir/new.md", text("old")), false, "normal to excluded");
        assertEquals(await exists(join(dir, "@eaDir/new.md")), false, "the excluded destination is not written");
        assertEquals(await Deno.readTextFile(join(dir, "old.md")), "old", "the old file is kept");
        assertEquals(await peer.rename("old.md", ".obsidian/new.md", text("old")), false, "normal to a dot name");
        assertEquals(await exists(join(dir, ".obsidian")), false, "the dot destination is not created");
        assertEquals(await exists(join(dir, "old.md")), true, "the old file is still kept");
        // The old name is excluded: the new file is stored, the excluded source is not touched.
        const looked = await recordingFileCalls(async () => {
            assertEquals(await peer.rename("@eaDir/src.md", "moved.md", text("src")), true, "excluded to normal stores the file");
        });
        assertEquals(touchesExcluded(looked).join(","), "", "the excluded source is not even looked at");
        assertEquals(await Deno.readTextFile(join(dir, "moved.md")), "src", "the new file has the content");
        assertEquals(await Deno.readTextFile(join(dir, "@eaDir/src.md")), "src", "the excluded source is kept");
        assertEquals(await peer.rename(".hidden/src.md", "moved2.md", text("src2")), true, "dot source to normal");
        assertEquals(await exists(join(dir, ".hidden/src.md")), true, "the dot source is kept");
        // A normal rename is unchanged.
        assertEquals(await peer.rename("old.md", "renamed.md", text("old")), true, "normal to normal");
        assertEquals(await exists(join(dir, "old.md")), false, "the old file is removed");
        assertEquals(await Deno.readTextFile(join(dir, "renamed.md")), "old", "the new file is stored");
        assertEquals(notices.length, 0, "no notice or failure is reported");
        await peer.stop();
    });
});

Deno.test("Hub.dispatch of an excluded change reports no failure, so the source has nothing to retry", async () => {
    await withDir(async (dir) => {
        const { peer, notices } = makePeer(dir, { group: "g" });
        const asked: string[] = [];
        const source = {
            config: { name: "src", group: "g" },
            cache: { clear: () => {} },
            currentPaths: (path: string) => (asked.push(path), Promise.resolve([] as string[])),
        } as unknown as Peer;
        const hub = new Hub({ peers: [] });
        hub.peers = [source, peer];
        await write(dir, "@eaDir/keep.jpg", "keep");
        // None of these throws.
        await hub.dispatch(source, "@eaDir/new.jpg", text("n"));
        await hub.dispatch(source, "A/.trash/new.md", text("n"));
        await hub.dispatch(source, "@eaDir/keep.jpg", false);
        await hub.dispatch(source, "old.md", text("n"), { previousPath: "@eaDir/keep.jpg" });
        await hub.dispatch(source, "@eaDir/moved.md", text("n"), { previousPath: "old2.md" });
        assertEquals(asked.length, 0, "the source is not even asked about an excluded path");
        assertEquals(await exists(join(dir, "@eaDir/new.jpg")), false, "nothing was written in @eaDir");
        assertEquals(await exists(join(dir, "A")), false, "nothing was written in a dot folder");
        assertEquals(await Deno.readTextFile(join(dir, "@eaDir/keep.jpg")), "keep", "the excluded file is kept");
        assertEquals(await Deno.readTextFile(join(dir, "old.md")), "n", "a move out of an excluded name stores the new file");
        assertEquals(notices.length, 0, "no notice");
        await peer.stop();
    });
});

Deno.test("ignore still only stops sending; the new options stop both directions", async () => {
    await withDir(async (dir) => {
        const { peer, plus } = makePeer(dir, { ignore: ["private/**"] });
        await write(dir, "private/mine.md", "mine");
        await write(dir, "@eaDir/t.jpg");
        await write(dir, "ok.md", "ok");
        assertEquals(await peer.rescan(dir, "test"), 1, "neither the ignored nor the excluded folder is sent");
        await waitFor(() => plus() === "+ok.md", "ok.md is sent");
        assertEquals(await peer.put("private/theirs.md", text("t")), true, "an incoming write at an ignored path is still applied");
        assertEquals(await Deno.readTextFile(join(dir, "private/theirs.md")), "t", "and written");
        assertEquals(await peer.put("@eaDir/theirs.jpg", text("t")), false, "an incoming write at an excluded path is not");
        assertEquals(await exists(join(dir, "@eaDir/theirs.jpg")), false, "and not written");
        await peer.stop();
    });
});

// ---- Interplay with the CouchDB peer's includeInternal ----

const couchdb = (extra: Record<string, unknown> = {}) =>
    ({
        type: "couchdb",
        name: "couch",
        group: "g",
        database: "db",
        username: "u",
        password: "p",
        url: "http://localhost:5984",
        passphrase: "",
        obfuscatePassphrase: "",
        baseDir: "",
        includeInternal: [".claude/**"],
        ...extra,
    }) as PeerConf;
const storage = (extra: Record<string, unknown> = {}) =>
    ({ type: "storage", name: "files", group: "g", baseDir: "./vault/", ...extra }) as PeerConf;

Deno.test("includeInternal and the default dot exclusion: the exclusion wins, and a notice says so", () => {
    const notices = internalFileExclusionNotices({ peers: [couchdb(), storage()] } as Config);
    assertEquals(notices.length, 1, "one notice for the pair");
    assertEquals(/includeInternal/.test(notices[0]) && /excludeDotNames/.test(notices[0]), true, "it names both options");
    assertEquals(/"couch"/.test(notices[0]) && /"files"/.test(notices[0]), true, "and both peers");
    assertEquals(/exclusion wins/.test(notices[0]), true, "and the precedence");
    assertEquals(internalFileExclusionNotices({ peers: [couchdb(), storage({ excludeDotNames: false })] } as Config).length, 0,
        "no notice when the dot rule is off");
    assertEquals(internalFileExclusionNotices({ peers: [couchdb(), storage({ excludeDotNames: false, excludeNames: [".claude"] })] } as Config).length, 1,
        "a dot name in excludeNames also cancels includeInternal");
    assertEquals(internalFileExclusionNotices({ peers: [couchdb({ includeInternal: undefined }), storage()] } as Config).length, 0,
        "no notice without includeInternal");
    assertEquals(internalFileExclusionNotices({ peers: [couchdb({ includeInternal: [] }), storage()] } as Config).length, 0,
        "no notice with an empty includeInternal");
    assertEquals(internalFileExclusionNotices({ peers: [couchdb({ group: "other" }), storage()] } as Config).length, 0,
        "no notice for a storage peer in another group");
    assertEquals(internalFileExclusionNotices({ peers: [couchdb({ group: undefined }), storage({ group: undefined })] } as Config).length, 1,
        "peers without a group are in the same group");
});

Deno.test("includeInternal files reach a storage peer only when the user turns the dot exclusion off, in both directions", async () => {
    await withDir(async (root) => {
        const received = (peer: PeerStorage, hub: Hub, source: Peer) => hub.dispatch(source, ".claude/settings.json", text("{}")).then(() => peer);
        const source = {
            config: { name: "couch", group: "g" },
            cache: { clear: () => {} },
            currentPaths: () => Promise.resolve([] as string[]),
        } as unknown as Peer;

        // Default: exclusion wins. An internal file from the CouchDB peer is not written ...
        const dirOn = join(root, "on");
        await Deno.mkdir(dirOn);
        const on = makePeer(dirOn, { group: "g" });
        const hubOn = new Hub({ peers: [] });
        hubOn.peers = [source, on.peer];
        await received(on.peer, hubOn, source);
        assertEquals(await exists(join(dirOn, ".claude")), false, "default: the internal file is not written");
        // ... and a file of the same name on disk is not sent.
        await write(dirOn, ".claude/local.json", "{}");
        await write(dirOn, "note.md", "n");
        assertEquals(await on.peer.rescan(dirOn, "test"), 1, "default: only the note is sent");
        await waitFor(() => on.plus() === "+note.md", "the note is sent");
        await on.peer.stop();

        // The user turns the dot exclusion off: both directions work.
        const dirOff = join(root, "off");
        await Deno.mkdir(dirOff);
        const off = makePeer(dirOff, { group: "g", excludeDotNames: false });
        const hubOff = new Hub({ peers: [] });
        hubOff.peers = [source, off.peer];
        await received(off.peer, hubOff, source);
        assertEquals(await Deno.readTextFile(join(dirOff, ".claude/settings.json")), "{}", "the internal file is written");
        await write(dirOff, ".claude/local.json", "{}");
        // The settings file was written by the put, so its record is current; the local one is sent.
        assertEquals(await off.peer.rescan(dirOff, "test"), 1, "a hidden file on disk is sent");
        await waitFor(() => off.plus() === "+.claude/local.json", "the hidden file is sent");
        await off.peer.stop();
    });
});
