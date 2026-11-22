// A folder or file which cannot be read, or a watcher which cannot be started, must not stop the
// peer: the path is left out with one notice, the rest is synchronised, the path is tried again by
// the periodic rescan, and nothing which cannot be read is taken as deleted.
// The permission errors are injected (PeerStorage.readDirectory, PeerStorage.get, Deno.stat and
// Deno.watchFs are replaced), so these tests give the same result on every OS. The one test with a
// real chmod is skipped on Windows and when run as root.
import chokidar from "chokidar";
import { join, resolve } from "@std/path";
import { LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";
import { PeerStorage } from "./PeerStorage.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function waitFor(condition: () => boolean, message: string, timeoutMs = 15_000) {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error(`Timed out while waiting: ${message}`);
        await sleep(10);
    }
}

// A Deno.watchFs stand-in which reports nothing until it is closed (or is made to fail).
class FakeDenoWatcher implements AsyncIterator<Deno.FsEvent>, AsyncIterable<Deno.FsEvent> {
    private result = Promise.withResolvers<IteratorResult<Deno.FsEvent>>();
    closed = false;
    next() {
        return this.result.promise;
    }
    [Symbol.asyncIterator]() {
        return this;
    }
    fail(error: unknown) {
        this.result.reject(error);
    }
    close() {
        this.closed = true;
        this.result.resolve({ done: true, value: undefined });
    }
    [Symbol.dispose]() {
        this.close();
    }
}

function permissionDenied(path: string) {
    return new Deno.errors.PermissionDenied(`Permission denied (os error 13), readdir '${path}'`);
}

function makePeer(baseDir: string, extra: Partial<PeerStorageConf> = {}) {
    const config: PeerStorageConf = {
        type: "storage",
        name: `unreadable-${crypto.randomUUID()}`,
        baseDir,
        scanOfflineChanges: true,
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
    peer.normalLog = (message, level) => {
        if (level === LOG_LEVEL_NOTICE) notices.push(message);
    };
    peer.debugLog = () => {};
    peer.sendLog = () => {};
    peer.receiveLog = () => {};
    const sent: string[] = [];
    const deleted: string[] = [];
    peer.dispatchToHub = (_source, path, data: FileData | false) => {
        if (data === false) deleted.push(path);
        else sent.push(path);
        return Promise.resolve();
    };
    // Directories (absolute paths) whose reading is refused.
    const denied = new Set<string>();
    peer.readDirectory = (path) => {
        if (denied.has(resolve(path))) {
            // The error is raised when the listing is read, as Deno.readDir does.
            return {
                [Symbol.asyncIterator]: () => ({ next: () => Promise.reject(permissionDenied(path)) }),
            };
        }
        return Deno.readDir(path);
    };
    const deny = (path: string) => void denied.add(resolve(path));
    const allow = (path: string) => void denied.delete(resolve(path));
    return { peer, sent, deleted, notices, deny, allow };
}

async function makeVault() {
    const dir = await Deno.makeTempDir({ prefix: "unreadable-" });
    await Deno.mkdir(join(dir, "locked", "sub"), { recursive: true });
    await Deno.mkdir(join(dir, "open"), { recursive: true });
    await Deno.writeTextFile(join(dir, "ok.md"), "ok");
    await Deno.writeTextFile(join(dir, "open", "c.md"), "c");
    await Deno.writeTextFile(join(dir, "locked", "a.md"), "a");
    await Deno.writeTextFile(join(dir, "locked", "sub", "b.md"), "b");
    return dir;
}

// Run `body` with Deno.watchFs replaced by one which reports nothing.
async function withQuietWatcher<T>(body: (watchers: FakeDenoWatcher[]) => Promise<T>): Promise<T> {
    const original = Deno.watchFs;
    const watchers: FakeDenoWatcher[] = [];
    Deno.watchFs = (() => {
        const watcher = new FakeDenoWatcher();
        watchers.push(watcher);
        return watcher as unknown as Deno.FsWatcher;
    }) as typeof Deno.watchFs;
    try {
        return await body(watchers);
    } finally {
        Deno.watchFs = original;
    }
}

const sorted = (values: string[]) => [...new Set(values)].sort().join(",");
const countOf = (notices: string[], text: string) => notices.filter((notice) => notice.includes(text)).length;

Deno.test("an unreadable folder is skipped at start with one notice, and the rest is synchronised", async () => {
    await withQuietWatcher(async () => {
        const dir = await makeVault();
        const { peer, sent, notices, deny } = makePeer(dir);
        deny(join(dir, "locked"));
        let starting: Promise<void> | undefined;
        try {
            starting = peer.start();
            await waitFor(() => peer.health().ok, "the watcher is ready");
            await waitFor(() => sorted(sent) === "ok.md,open/c.md", "the readable files are sent");
            assertEquals(countOf(notices, "Cannot read folder"), 1, "one notice for the folder");
            assertEquals(notices.some((n) => n.includes(join(dir, "locked"))), true, "which says what to look at");
            assertEquals(peer.unreadablePaths().length, 1, "the folder is remembered");
        } finally {
            await peer.stop();
            await starting;
            await Deno.remove(dir, { recursive: true });
        }
    });
});

Deno.test("an unreadable folder is tried at each rescan, reported once, and synchronised once readable", async () => {
    await withQuietWatcher(async () => {
        const dir = await makeVault();
        const { peer, sent, notices, deny, allow } = makePeer(dir);
        deny(join(dir, "locked"));
        let starting: Promise<void> | undefined;
        try {
            starting = peer.start();
            await waitFor(() => peer.health().ok, "the watcher is ready");
            await waitFor(() => sorted(sent) === "ok.md,open/c.md", "the readable files are sent");
            for (let round = 0; round < 3; round++) await peer.rescan(dir, "periodic");
            await sleep(600);
            assertEquals(countOf(notices, "Cannot read folder"), 1, "the same folder is not reported again");
            assertEquals(sorted(sent), "ok.md,open/c.md", "nothing from the folder yet");

            allow(join(dir, "locked"));
            await peer.rescan(dir, "periodic");
            await waitFor(() => sorted(sent) === "locked/a.md,locked/sub/b.md,ok.md,open/c.md", "the folder is sent");
            assertEquals(peer.unreadablePaths().length, 0, "it is no longer remembered");
            assertEquals(countOf(notices, "can be read now"), 1, "its recovery is reported once");
            assertEquals(sent.length, 4, "each file was sent exactly once");

            // Reported again if it fails again: that is news.
            deny(join(dir, "locked"));
            await peer.rescan(dir, "periodic");
            assertEquals(countOf(notices, "Cannot read folder"), 2, "a new failure is reported");
        } finally {
            await peer.stop();
            await starting;
            await Deno.remove(dir, { recursive: true });
        }
    });
});

Deno.test("files below a folder which cannot be listed are not taken as deleted", async () => {
    await withQuietWatcher(async () => {
        const dir = await makeVault();
        const { peer, sent, deleted, notices, deny, allow } = makePeer(dir);
        let starting: Promise<void> | undefined;
        try {
            starting = peer.start();
            await waitFor(() => peer.health().ok, "the watcher is ready");
            await waitFor(() => sent.length === 4, "everything is sent and known");

            // The files really are gone, but the folder cannot be listed: that proves nothing.
            await Deno.remove(join(dir, "locked"), { recursive: true });
            await Deno.mkdir(join(dir, "locked"));
            deny(join(dir, "locked"));
            peer.processFile({ kind: "modify", paths: [join(dir, "locked")] });
            await sleep(1_000);
            assertEquals(deleted.length, 0, `no deletion is sent for an unreadable folder: ${deleted}`);
            assertEquals(countOf(notices, "Cannot read folder"), 1, "reported");

            // Readable again, and really empty: now the deletions are real.
            allow(join(dir, "locked"));
            peer.processFile({ kind: "modify", paths: [join(dir, "locked")] });
            await waitFor(() => sorted(deleted) === "locked/a.md,locked/sub/b.md", "the real deletions are sent");
        } finally {
            await peer.stop();
            await starting;
            await Deno.remove(dir, { recursive: true });
        }
    });
});

Deno.test("a file which cannot be read is reported once, is not a failed send, and is sent once readable", async () => {
    await withQuietWatcher(async () => {
        const dir = await makeVault();
        const { peer, sent, notices } = makePeer(dir);
        const original = peer.get.bind(peer);
        let refuse = true;
        peer.get = (path, access) => {
            if (refuse && path === "locked/a.md") return Promise.reject(new Deno.errors.PermissionDenied(`open '${path}'`));
            return original(path, access);
        };
        let starting: Promise<void> | undefined;
        try {
            starting = peer.start();
            await waitFor(() => peer.health().ok, "the watcher is ready");
            await waitFor(() => sorted(sent) === "locked/sub/b.md,ok.md,open/c.md", "the other files are sent");
            for (let round = 0; round < 3; round++) await peer.rescan(dir, "periodic");
            await sleep(600);
            assertEquals(countOf(notices, "Cannot read file"), 1, "the file is reported once");
            assertEquals(countOf(notices, "File read failed"), 0, "it is not reported as a failed read each time");
            assertEquals(peer.unreadablePaths().join(","), join(dir, "locked", "a.md"), "remembered");

            refuse = false;
            await peer.rescan(dir, "periodic");
            await waitFor(() => sent.includes("locked/a.md"), "the file is sent once readable");
            assertEquals(peer.unreadablePaths().length, 0, "forgotten");
        } finally {
            await peer.stop();
            await starting;
            await Deno.remove(dir, { recursive: true });
        }
    });
});

Deno.test("a permission error while inspecting a file at start does not stop the peer", async () => {
    await withQuietWatcher(async () => {
        const dir = await makeVault();
        const { peer, sent, notices } = makePeer(dir);
        const blocked = join(dir, "open", "c.md");
        const originalStat = Deno.stat;
        Deno.stat = ((path: string | URL) =>
            resolve(String(path)) === resolve(blocked)
                ? Promise.reject(new Deno.errors.PermissionDenied(`stat '${path}'`))
                : originalStat(path)) as typeof Deno.stat;
        let starting: Promise<void> | undefined;
        try {
            starting = peer.start();
            await waitFor(() => peer.health().ok, "the watcher is ready");
            await waitFor(() => sorted(sent) === "locked/a.md,locked/sub/b.md,ok.md", "the other files are sent");
            assertEquals(countOf(notices, "Cannot read file"), 1, "reported once");
        } finally {
            Deno.stat = originalStat;
            await peer.stop();
            await starting;
            await Deno.remove(dir, { recursive: true });
        }
    });
});

for (const useChokidar of [false, true]) {
    Deno.test(`with path identity 1 and useChokidar ${useChokidar}, an unreadable folder is skipped by the scans`, async () => {
        const originalChokidar = chokidar.watch;
        const handlers = new Map<string, ((...args: unknown[]) => unknown)[]>();
        const fakeChokidar = {
            on(event: string, handler: (...args: unknown[]) => unknown) {
                handlers.set(event, [...(handlers.get(event) ?? []), handler]);
                return fakeChokidar;
            },
            close: () => Promise.resolve(),
        };
        chokidar.watch = (() => fakeChokidar as unknown as ReturnType<typeof chokidar.watch>) as typeof chokidar.watch;
        try {
            await withQuietWatcher(async () => {
                const dir = await makeVault();
                const { peer, sent, notices, deny, allow } = makePeer(dir, {
                    useChokidar,
                    pathNormalizationVersion: 1,
                    handleFilenameCaseSensitive: true,
                });
                deny(join(dir, "locked"));
                let starting: Promise<void> | undefined;
                try {
                    starting = peer.start();
                    if (useChokidar) {
                        await starting;
                        for (const handler of handlers.get("ready") ?? []) handler();
                    } else {
                        await waitFor(() => peer.health().ok, "the watcher is ready");
                    }
                    await waitFor(() => sorted(sent) === "ok.md,open/c.md", "the readable files are sent");
                    assertEquals(countOf(notices, "Cannot read folder"), 1, "one notice");
                    await peer.rescan(dir, "periodic");
                    assertEquals(countOf(notices, "Cannot read folder"), 1, "not repeated by a rescan");
                    allow(join(dir, "locked"));
                    await peer.rescan(dir, "periodic");
                    await waitFor(() => sent.length === 4, "the folder is sent once readable");
                } finally {
                    await peer.stop();
                    await starting;
                    await Deno.remove(dir, { recursive: true });
                }
            });
        } finally {
            chokidar.watch = originalChokidar;
        }
    });
}

Deno.test("when Deno.watchFs cannot start, the peer lives on its rescans and starts the watcher again", async () => {
    const original = Deno.watchFs;
    const watchers: FakeDenoWatcher[] = [];
    let calls = 0;
    let refuse = true;
    Deno.watchFs = (() => {
        calls++;
        if (refuse) throw permissionDenied("vault/locked");
        const watcher = new FakeDenoWatcher();
        watchers.push(watcher);
        return watcher as unknown as Deno.FsWatcher;
    }) as typeof Deno.watchFs;
    const dir = await makeVault();
    const { peer, sent, notices } = makePeer(dir, { rescanIntervalMinutes: 0.001 });
    try {
        // start() returns: there is no watcher to wait for.
        await peer.start();
        assertEquals(countOf(notices, "folder watcher could not be started"), 1, "the problem is reported");
        assertEquals(peer.health().ok, false, "there is no watcher");
        assertEquals(peer.health().detail, "rescan only (no watcher)", "and it says what the peer is doing");
        const probed = await peer.probeHealth();
        assertEquals(probed.restartWorthy, false, "a restart would not help");
        await waitFor(() => sorted(sent) === "locked/a.md,locked/sub/b.md,ok.md,open/c.md", "the start-up scan sent the files");

        // A file added meanwhile is found by the periodic rescan.
        await Deno.writeTextFile(join(dir, "open", "new.md"), "new");
        await waitFor(() => sent.includes("open/new.md"), "the periodic rescan sends the new file");
        await waitFor(() => calls >= 3, "the rescans try the watcher again");
        assertEquals(countOf(notices, "folder watcher could not be started"), 1, "the same problem is not reported again");

        // The OS accepts the watch now.
        refuse = false;
        await waitFor(() => peer.health().ok, "the watcher is running again");
        assertEquals(countOf(notices, "running again"), 1, "reported");
        await Deno.writeTextFile(join(dir, "open", "later.md"), "later");
        await waitFor(() => sent.includes("open/later.md"), "files are still found");
    } finally {
        await peer.stop();
        await sleep(50);
        Deno.watchFs = original;
        await Deno.remove(dir, { recursive: true });
    }
    assertEquals(watchers.every((watcher) => watcher.closed), true, "stop() closed the watcher which was started again");
});

Deno.test("without a watcher, a rescan timer which fires during the start-up inventory does not end the rescans", async () => {
    const original = Deno.watchFs;
    Deno.watchFs = (() => {
        throw permissionDenied("vault/locked");
    }) as typeof Deno.watchFs;
    const dir = await makeVault();
    const { peer, sent } = makePeer(dir, { rescanIntervalMinutes: 0.001 });
    // The inventory of the root takes a while.
    const release = Promise.withResolvers<void>();
    const readDirectory = peer.readDirectory;
    peer.readDirectory = (path) => {
        if (resolve(path) !== resolve(dir)) return readDirectory(path);
        return {
            async *[Symbol.asyncIterator]() {
                await release.promise;
                yield* readDirectory(path);
            },
        };
    };
    try {
        const starting = peer.start();
        await sleep(400); // the rescan timer fires several times meanwhile: nothing is ready yet
        release.resolve();
        await starting;
        await waitFor(() => sorted(sent) === "locked/a.md,locked/sub/b.md,ok.md,open/c.md", "the start-up scan sent the files");
        await Deno.writeTextFile(join(dir, "open", "new.md"), "new");
        await waitFor(() => sent.includes("open/new.md"), "a periodic rescan still runs");
    } finally {
        release.resolve();
        await peer.stop();
        Deno.watchFs = original;
        await Deno.remove(dir, { recursive: true });
    }
});

Deno.test("when the watcher's events fail, the peer lives on its rescans and starts the watcher again", async () => {
    const original = Deno.watchFs;
    const watchers: FakeDenoWatcher[] = [];
    Deno.watchFs = (() => {
        const watcher = new FakeDenoWatcher();
        watchers.push(watcher);
        return watcher as unknown as Deno.FsWatcher;
    }) as typeof Deno.watchFs;
    const dir = await makeVault();
    const { peer, sent, notices } = makePeer(dir, { rescanIntervalMinutes: 0.001 });
    try {
        const starting = peer.start();
        await waitFor(() => peer.health().ok, "the watcher is ready");
        watchers[0].fail(permissionDenied("vault/locked"));
        await starting; // the event loop ended, and start() reports it by returning
        assertEquals(countOf(notices, "folder watcher failed"), 1, "the failure is reported");
        // The rescan notices that there is no watcher, and starts one again.
        await waitFor(() => watchers.length >= 2 && peer.health().ok, "a new watcher is running");
        assertEquals(watchers[0].closed, true, "the failed watcher was closed");
        await Deno.writeTextFile(join(dir, "open", "after.md"), "after");
        await waitFor(() => sent.includes("open/after.md"), "files are found after the failure");
    } finally {
        await peer.stop();
        await sleep(50);
        Deno.watchFs = original;
        await Deno.remove(dir, { recursive: true });
    }
});

Deno.test("a folder which became readable gets a new watcher", async () => {
    await withQuietWatcher(async (watchers) => {
        const dir = await makeVault();
        const { peer, sent, deny, allow } = makePeer(dir, { rescanIntervalMinutes: 0.001 });
        deny(join(dir, "locked"));
        let starting: Promise<void> | undefined;
        try {
            starting = peer.start();
            await waitFor(() => peer.health().ok, "the watcher is ready");
            await sleep(300);
            assertEquals(watchers.length, 1, "nothing is renewed while the folder is unreadable");
            allow(join(dir, "locked"));
            await waitFor(() => watchers.length === 2, "a new watcher is created for the readable folder");
            await waitFor(() => sent.length === 4 && peer.health().ok, "the folder is sent and watched");
            assertEquals(watchers[0].closed, true, "the previous watcher is closed");
            await sleep(300);
            assertEquals(watchers.length, 2, "and it is renewed once");
        } finally {
            await peer.stop();
            await starting;
            await Deno.remove(dir, { recursive: true });
        }
    });
});

const isRoot = Deno.build.os !== "windows" && Deno.uid() === 0;
Deno.test({
    name: "real chmod 000: the readable files are synchronised and the folder is skipped",
    ignore: Deno.build.os === "windows" || isRoot,
    fn: async () => {
        await withQuietWatcher(async () => {
            const dir = await makeVault();
            const locked = join(dir, "locked");
            const { peer, sent, deleted, notices } = makePeer(dir);
            // Nothing injected: the folder is really unreadable.
            peer.readDirectory = (path) => Deno.readDir(path);
            await Deno.chmod(locked, 0o000);
            let starting: Promise<void> | undefined;
            try {
                starting = peer.start();
                await waitFor(() => peer.health().ok, "the watcher is ready");
                await waitFor(() => sorted(sent) === "ok.md,open/c.md", "the readable files are sent");
                assertEquals(countOf(notices, "Cannot read folder"), 1, "reported once");
                await peer.rescan(dir, "periodic");
                await Deno.chmod(locked, 0o755);
                await peer.rescan(dir, "periodic");
                await waitFor(() => sent.length === 4, "sent once readable");
                assertEquals(deleted.length, 0, "nothing was taken as deleted");
            } finally {
                await Deno.chmod(locked, 0o755).catch(() => {});
                await peer.stop();
                await starting;
                await Deno.remove(dir, { recursive: true });
            }
        });
    },
});
