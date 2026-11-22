// Rescans recover files the watcher did not report: a new folder is rescanned shortly after
// Chokidar reports it, and the whole folder is rescanned periodically. A rescan sends only
// changed files, skips files whose send is already queued, and never deletes.
import chokidar from "chokidar";
import { join } from "@std/path";
import { PeerStorage } from "./PeerStorage.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

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

function makePeer(baseDir: string, extra: Partial<PeerStorageConf> = {}) {
    const config: PeerStorageConf = {
        type: "storage",
        name: `rescan-${crypto.randomUUID()}`,
        baseDir,
        scanOfflineChanges: false,
        useChokidar: true,
        ...extra,
    };
    const peer = new PeerStorage(config, () => Promise.resolve());
    // Keep file records in memory, without the deferred save timer of the real state.
    const settings = new Map<string, string>();
    peer.setSetting = (key, value) => {
        settings.set(key, value);
        return Promise.resolve();
    };
    peer.getSetting = (key) => Promise.resolve(settings.get(key) ?? null);
    peer.setSettingLater = (key, value) => void peer.setSetting(key, value);
    peer.normalLog = () => {};
    peer.debugLog = () => {};
    peer.sendLog = () => {};
    const sent: string[] = [];
    const deleted: string[] = [];
    peer.dispatchToHub = (_source, path, data: FileData | false) => {
        if (data === false) deleted.push(path);
        else sent.push(path);
        return Promise.resolve();
    };
    return { peer, sent, deleted };
}

async function withFakeChokidar(
    extra: Partial<PeerStorageConf>,
    body: (ctx: { dir: string; peer: PeerStorage; sent: string[]; watcher: FakeWatcher }) => Promise<void>,
) {
    const original = chokidar.watch;
    const dir = await Deno.makeTempDir({ prefix: "rescan-" });
    const watcher = new FakeWatcher();
    chokidar.watch = (() => watcher as unknown as ReturnType<typeof chokidar.watch>) as typeof chokidar.watch;
    const { peer, sent } = makePeer(dir, extra);
    try {
        await peer.start();
        await body({ dir, peer, sent, watcher });
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
        sent: string[];
        deleted: string[];
        starting: Promise<void>;
    }) => Promise<void>,
) {
    const original = Deno.watchFs;
    const dir = await Deno.makeTempDir({ prefix: "rescan-deno-" });
    Deno.watchFs = (() => new QuietDenoWatcher() as unknown as Deno.FsWatcher) as typeof Deno.watchFs;
    const { peer, sent, deleted } = makePeer(dir, { useChokidar: false, ...extra });
    let starting: Promise<void> | undefined;
    try {
        starting = peer.start();
        await waitFor(() => peer.health().ok, "the Deno watcher is ready");
        await body({ dir, peer, sent, deleted, starting });
    } finally {
        await peer.stop();
        await starting;
        Deno.watchFs = original;
        await Deno.remove(dir, { recursive: true });
    }
}

Deno.test("files in a new folder that the watcher did not report are sent by the new-folder rescan", async () => {
    await withFakeChokidar({ rescanIntervalMinutes: 0 }, async ({ dir, sent, watcher }) => {
        watcher.emit("ready");
        await Deno.mkdir(join(dir, "A/photos"), { recursive: true });
        await Deno.writeTextFile(join(dir, "A/photos/1.md"), "one");
        await Deno.writeTextFile(join(dir, "A/photos/2.md"), "two");
        watcher.emit("addDir", join(dir, "A")); // only the folder event arrives, no "add" for the files
        await waitFor(() => sent.length >= 2, "both missed files are sent");
        assertEquals([...sent].sort().join(","), "A/photos/1.md,A/photos/2.md", "both missed files are sent");
    });
});

Deno.test("addDir events before the watcher is ready are ignored (the initial scan covers them)", async () => {
    await withFakeChokidar({ rescanIntervalMinutes: 0 }, async ({ dir, sent, watcher }) => {
        await Deno.mkdir(join(dir, "B"));
        await Deno.writeTextFile(join(dir, "B/x.md"), "x");
        watcher.emit("addDir", join(dir, "B"));
        await sleep(2_800);
        assertEquals(sent.length, 0, "nothing is sent before ready");
    });
});

Deno.test("an ignored new folder is not rescanned", async () => {
    await withFakeChokidar({ rescanIntervalMinutes: 0, ignore: ["**/private"] }, async ({ dir, sent, watcher }) => {
        watcher.emit("ready");
        await Deno.mkdir(join(dir, "private"));
        await Deno.writeTextFile(join(dir, "private/secret.md"), "secret");
        watcher.emit("addDir", join(dir, "private"));
        await sleep(2_800);
        assertEquals(sent.length, 0, "nothing inside an ignored folder is read");
    });
});

Deno.test("the periodic rescan sends a missed file once and does not resend unchanged files", async () => {
    // 0.002 min = 120 ms, randomised to 90..150 ms
    await withFakeChokidar({ rescanIntervalMinutes: 0.002 }, async ({ dir, sent, watcher }) => {
        await Deno.writeTextFile(join(dir, "missed.md"), "m");
        watcher.emit("ready");
        await waitFor(() => sent.length === 1, "the missed file is sent");
        await sleep(1_200);
        assertEquals(sent.join(","), "missed.md", "sent exactly once across several rescans");
        await Deno.writeTextFile(join(dir, "missed.md"), "changed content");
        await waitFor(() => sent.length === 2, "the edit is sent");
        await sleep(600);
        assertEquals(sent.join(","), "missed.md,missed.md", "a later unreported edit is sent again, once");
    });
});

Deno.test("files already reported by the watcher are not sent again by a rescan", async () => {
    await withFakeChokidar({ rescanIntervalMinutes: 0 }, async ({ dir, peer, sent, watcher }) => {
        watcher.emit("ready");
        const path = join(dir, "seen.md");
        await Deno.writeTextFile(path, "s");
        await peer.dispatch(path); // what the "add" handler does
        await waitFor(() => sent.length === 1, "the reported file is sent");
        await sleep(100);
        const queued = await peer.rescan(dir, "test");
        await sleep(400);
        assertEquals(queued, 0, "rescan finds nothing new");
        assertEquals(sent.join(","), "seen.md", "sent once");
    });
});

Deno.test("rescanIntervalMinutes 0 disables, default is 60 minutes randomised to 45..75", () => {
    const { peer } = makePeer("/tmp", { rescanIntervalMinutes: 0 });
    assertEquals(peer.rescanIntervalMs(), 0, "disabled");
    const { peer: byDefault } = makePeer("/tmp");
    let lowest = Infinity;
    let highest = 0;
    for (let i = 0; i < 200; i++) {
        const ms = byDefault.rescanIntervalMs();
        if (ms < 45 * 60_000 || ms > 75 * 60_000) throw new Error(`out of range: ${ms}`);
        lowest = Math.min(lowest, ms);
        highest = Math.max(highest, ms);
    }
    assertEquals(highest - lowest > 5 * 60_000, true, "the interval is randomised");
});

Deno.test("with Deno.watchFs a periodic rescan sends a file the watcher never reported, and never deletes", async () => {
    await withQuietDenoWatcher({ rescanIntervalMinutes: 0.002 }, async ({ dir, sent, deleted }) => {
        await Deno.writeTextFile(join(dir, "missed.md"), "m");
        await Deno.writeTextFile(join(dir, "gone.md"), "g");
        await waitFor(() => sent.length === 2, "both files are found by a rescan");
        await Deno.remove(join(dir, "gone.md"));
        await sleep(1_000); // several more rescans
        assertEquals([...sent].sort().join(","), "gone.md,missed.md", "each file is sent once");
        assertEquals(deleted.length, 0, "a rescan never infers a deletion");
    });
});

Deno.test("with Deno.watchFs a rescan finds files in a renamed folder only once together with the event", async () => {
    // processFile() reconciles the renamed folder at once; the rescan afterwards finds nothing new.
    await withQuietDenoWatcher({ rescanIntervalMinutes: 0 }, async ({ dir, peer, sent }) => {
        await Deno.mkdir(join(dir, "moved/deep"), { recursive: true });
        await Deno.writeTextFile(join(dir, "moved/deep/note.md"), "body");
        peer.processFile({ kind: "create", paths: [join(dir, "moved")] });
        await waitFor(() => sent.length === 1, "the folder event sends its file");
        await sleep(100);
        const queued = await peer.rescan(dir, "test");
        await sleep(500);
        assertEquals(queued, 0, "the rescan has nothing left to queue");
        assertEquals(sent.join(","), "moved/deep/note.md", "the file is sent once");
    });
});

Deno.test("an overlapping periodic rescan is skipped", async () => {
    await withQuietDenoWatcher({ rescanIntervalMinutes: 0.002 }, async ({ peer }) => {
        let calls = 0;
        peer.rescan = () => {
            calls++;
            return Promise.resolve(0);
        };
        // deno-lint-ignore no-explicit-any
        const internal = peer as any;
        internal.fullScanRunning = true;
        peer.scheduleFullRescan();
        await sleep(600);
        assertEquals(calls, 0, "no rescan starts while another one runs");
        internal.fullScanRunning = false;
        await waitFor(() => calls > 0, "rescans resume once the previous one has finished");
    });
});

Deno.test("a rescan does not queue files that are already waiting to be sent", async () => {
    const dir = await Deno.makeTempDir({ prefix: "rescan-pending-" });
    try {
        const { peer } = makePeer(dir, { useChokidar: false, rescanIntervalMinutes: 0, maxConcurrentSends: 1 });
        const sends: string[] = [];
        peer.dispatchToHub = async (_source, path: string) => {
            await sleep(30);
            sends.push(path);
        };
        for (let i = 0; i < 20; i++) await Deno.writeTextFile(join(dir, `n${i}.md`), `note ${i}`);
        // The watcher reports all 20 at once; with one slot most of them wait.
        for (let i = 0; i < 20; i++) await peer.dispatch(join(dir, `n${i}.md`));
        await sleep(400);
        assertEquals(peer.isSendPending("n19.md"), true, "a waiting file is known as pending");
        const queuedByRescan = await peer.rescan(dir, "periodic");
        assertEquals(queuedByRescan, 0, "the rescan queued nothing");
        await waitFor(() => sends.length >= 20, "all files are sent");
        await sleep(400);
        assertEquals(sends.length, 20, `each file sent exactly once (sent ${sends.length})`);
        assertEquals(peer.isSendPending("n19.md"), false, "nothing pending afterwards");
        await peer.stop();
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
});

Deno.test("an edit reported while a file is being sent is still sent afterwards", async () => {
    const dir = await Deno.makeTempDir({ prefix: "rescan-edit-" });
    try {
        const { peer } = makePeer(dir, { useChokidar: false, rescanIntervalMinutes: 0, maxConcurrentSends: 1 });
        const got: string[] = [];
        peer.dispatchToHub = async (_source, path: string, data: FileData | false) => {
            await sleep(200);
            got.push(`${path}=${data ? (data.data as string[]).join("") : ""}`);
        };
        const path = join(dir, "n0.md");
        await Deno.writeTextFile(path, "first");
        await peer.dispatch(path);
        await sleep(450); // the first send is in progress
        await Deno.writeTextFile(path, "edited");
        await peer.dispatch(path); // a watcher "change" event is not skipped
        await waitFor(() => got[got.length - 1] === "n0.md=edited", `the edit was sent: ${got.join(" | ")}`);
        await peer.stop();
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
});

// Real Chokidar: a folder with files is moved into a folder that was created a moment earlier.
Deno.test({
    name: "real chokidar: files moved into a just-created folder reach the hub",
    sanitizeOps: false,
    sanitizeResources: false,
    fn: async () => {
        const root = await Deno.makeTempDir({ prefix: "rescan-real-" });
        const vault = join(root, "vault");
        const staging = join(root, "staging");
        await Deno.mkdir(vault);
        await Deno.mkdir(join(staging, "photos"), { recursive: true });
        for (let i = 0; i < 20; i++) await Deno.writeTextFile(join(staging, "photos", `${i}.md`), `p${i}`);
        const { peer, sent } = makePeer(vault, { rescanIntervalMinutes: 0 });
        try {
            await peer.start();
            await sleep(1_500); // watcher ready
            await Deno.mkdir(join(vault, "LT"));
            await Deno.rename(join(staging, "photos"), join(vault, "LT", "photos"));
            await waitFor(
                () => new Set(sent.filter((path) => path.startsWith("LT/photos/"))).size === 20,
                "all 20 files are sent",
                15_000,
            );
        } finally {
            await peer.stop();
            await Deno.remove(root, { recursive: true });
        }
    },
});
