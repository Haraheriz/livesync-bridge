import { join } from "@std/path";
import { PeerStorage } from "./PeerStorage.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

// A received document whose content is already on disk must not be written again: a
// catch-up from the start of the changes feed re-delivers every document, and a rewrite
// would change the mtime, cost a snapshot copy and make the watcher send the file back.

const ON_DISK_MTIME = 1_600_000_000_000;
const DELIVERED_MTIME = 1_700_000_002_000;

function assert(condition: unknown, message: string) {
    if (!condition) throw new Error(message);
}

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}

function textData(text: string, mtime = DELIVERED_MTIME): FileData {
    return { ctime: mtime, mtime, size: new TextEncoder().encode(text).byteLength, data: [text] };
}

// File records are kept in memory (the real queue defers saving by a timer, which Deno
// reports as a leaked timer), and everything the peer logs or runs is recorded.
function makePeer(baseDir: string) {
    const config: PeerStorageConf = {
        type: "storage",
        name: `unchanged-${crypto.randomUUID()}`,
        baseDir,
        scanOfflineChanges: false,
        useChokidar: false,
    };
    const peer = new PeerStorage(config, () => Promise.resolve());
    const settings = new Map<string, string>();
    peer.setSetting = (key, value) => {
        settings.set(key, value);
        return Promise.resolve();
    };
    peer.getSetting = (key) => Promise.resolve(settings.get(key) ?? null);
    peer.setSettingLater = (key, value) => void peer.setSetting(key, value);
    const received: string[] = [];
    const debug: string[] = [];
    const notices: string[] = [];
    const scripts: string[] = [];
    peer.receiveLog = (message) => void received.push(message);
    peer.debugLog = (message) => void debug.push(message);
    peer.normalLog = (message) => void notices.push(message);
    peer.runScript = (filename) => {
        scripts.push(filename);
        return Promise.resolve(true);
    };
    return { peer, received, debug, notices, scripts };
}

// Creates a file on disk whose mtime differs from the delivered one.
async function seed(path: string, content: string) {
    await Deno.mkdir(join(path, ".."), { recursive: true });
    await Deno.writeTextFile(path, content);
    const mtime = new Date(ON_DISK_MTIME);
    await Deno.utime(path, mtime, mtime);
}

async function withDir(run: (dir: string) => Promise<void>) {
    const dir = await Deno.makeTempDir({ prefix: "peer-storage-unchanged-" });
    try {
        await run(dir);
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
}

const mtimeOf = async (path: string) => (await Deno.stat(path)).mtime?.getTime();

Deno.test("PeerStorage does not rewrite a file whose content is already identical", async () => {
    await withDir(async (dir) => {
        const { peer, received, debug, notices, scripts } = makePeer(dir);
        const path = join(dir, "note.md");
        await seed(path, "same text");
        assert(await peer.isChanged("note.md"), "a file without a record is changed before the put");

        const data = textData("same text");
        assertEquals(await peer.put("note.md", data), true, "an identical file counts as in place, so a rename can go on");

        assertEquals(await mtimeOf(path), ON_DISK_MTIME, "the modification time on disk must not change");
        assertEquals(await Deno.readTextFile(path), "same text", "the content stays");
        assertEquals(received.filter((m) => m.includes("saved")).length, 0, "nothing is logged as saved");
        assertEquals(scripts.length, 0, "the processor script must not run");
        assertEquals(debug.filter((m) => m.endsWith("note.md unchanged; not rewritten")).length, 1, "one debug line explains why");
        assertEquals(notices.length, 0, "no notice is given");
        // The record equals the file on disk, so neither scan sends the file back.
        assertEquals(await peer.isChanged("note.md"), false, "the file must not be reported as changed afterwards");
        // The content is remembered, so the watcher event for it is a repeat.
        assertEquals(await peer.isRepeating("note.md", data), true, "the content is remembered as a repeat");
        // A peer without Chokidar must know the file for its rescans.
        assert((peer as unknown as { knownFiles: Set<string> }).knownFiles.size === 1, "the file is known to the peer");
        await peer.stop();
    });
});

Deno.test("PeerStorage does not rewrite files beyond the capacity of the repeat cache", async () => {
    await withDir(async (dir) => {
        const { peer, received } = makePeer(dir);
        const count = 350; // more than the 300 entries the cache keeps
        for (let i = 0; i < count; i++) await seed(join(dir, `d${i % 7}`, `n${i}.md`), `content ${i}`);
        for (let i = 0; i < count; i++) {
            assertEquals(await peer.put(`d${i % 7}/n${i}.md`, textData(`content ${i}`)), true, `n${i}.md is in place`);
        }
        // Delivering everything again, with no cache entry left for any path.
        peer.cache.clear();
        for (let i = 0; i < count; i++) {
            assertEquals(await peer.put(`d${i % 7}/n${i}.md`, textData(`content ${i}`)), true, `n${i}.md is in place again`);
        }
        assertEquals(received.filter((m) => m.includes("saved")).length, 0, "no file is written");
        for (let i = 0; i < count; i++) {
            const path = join(dir, `d${i % 7}`, `n${i}.md`);
            assertEquals(await mtimeOf(path), ON_DISK_MTIME, `n${i}.md keeps its mtime`);
            assertEquals(await peer.isChanged(`d${i % 7}/n${i}.md`), false, `n${i}.md is not reported as changed`);
        }
        await peer.stop();
    });
});

Deno.test("PeerStorage writes different content of the same size", async () => {
    await withDir(async (dir) => {
        const { peer, received, debug, scripts } = makePeer(dir);
        const path = join(dir, "note.md");
        await seed(path, "aaaa");
        assertEquals(await peer.put("note.md", textData("aaab")), true, "saved");
        assertEquals(await Deno.readTextFile(path), "aaab", "the new content is on disk");
        assertEquals(await mtimeOf(path), DELIVERED_MTIME, "the delivered mtime is set");
        assertEquals(received.filter((m) => m.includes("saved")).length, 1, "logged as saved");
        assertEquals(debug.filter((m) => m.includes("unchanged")).length, 0, "not reported as unchanged");
        assertEquals(scripts.length, 1, "the processor script runs");
        assertEquals(await peer.isChanged("note.md"), false, "the record matches the new file");
        await peer.stop();
    });
});

Deno.test("PeerStorage writes content of a different size", async () => {
    await withDir(async (dir) => {
        const { peer } = makePeer(dir);
        const path = join(dir, "note.md");
        await seed(path, "short");
        assertEquals(await peer.put("note.md", textData("a longer text")), true, "saved (longer)");
        assertEquals(await Deno.readTextFile(path), "a longer text", "the longer content is on disk");
        assertEquals(await peer.put("note.md", textData("tiny")), true, "saved (shorter)");
        assertEquals(await Deno.readTextFile(path), "tiny", "the file is truncated to the shorter content");
        assertEquals(await mtimeOf(path), DELIVERED_MTIME, "the delivered mtime is set");
        await peer.stop();
    });
});

Deno.test("PeerStorage writes a file which does not exist yet", async () => {
    await withDir(async (dir) => {
        const { peer, received } = makePeer(dir);
        assertEquals(await peer.put("new/deep/note.md", textData("fresh")), true, "saved");
        assertEquals(await Deno.readTextFile(join(dir, "new", "deep", "note.md")), "fresh", "created with its content");
        assertEquals(received.filter((m) => m.includes("saved")).length, 1, "logged as saved");
        await peer.stop();
    });
});

Deno.test("PeerStorage writes binary content and leaves identical binary content alone", async () => {
    await withDir(async (dir) => {
        const { peer, received } = makePeer(dir);
        const path = join(dir, "file.bin");
        const bytes = new Uint8Array([0, 255, 1, 254, 2, 253]);
        await Deno.writeFile(path, bytes);
        const mtime = new Date(ON_DISK_MTIME);
        await Deno.utime(path, mtime, mtime);
        const same: FileData = { ctime: DELIVERED_MTIME, mtime: DELIVERED_MTIME, size: bytes.length, data: bytes.slice() };
        assertEquals(await peer.put("file.bin", same), true, "identical binary content is in place");
        assertEquals(await mtimeOf(path), ON_DISK_MTIME, "identical binary content is not rewritten");
        const other = bytes.slice();
        other[5] = 0;
        assertEquals(await peer.put("file.bin", { ...same, data: other }), true, "different binary content is saved");
        assertEquals((await Deno.readFile(path)).join(","), other.join(","), "the new bytes are on disk");
        assertEquals(received.filter((m) => m.includes("saved")).length, 1, "only the different content is written");
        await peer.stop();
    });
});

Deno.test("PeerStorage rename removes the previous path when the destination is already identical", async () => {
    await withDir(async (dir) => {
        const { peer, received } = makePeer(dir);
        await seed(join(dir, "old.md"), "moved text");
        await seed(join(dir, "new.md"), "moved text");
        assertEquals(await peer.rename("old.md", "new.md", textData("moved text")), true, "the rename is accepted");
        assertEquals(await mtimeOf(join(dir, "new.md")), ON_DISK_MTIME, "the destination is not rewritten");
        assertEquals(received.filter((m) => m.includes("saved")).length, 0, "nothing is saved");
        let oldExists = true;
        try {
            await Deno.stat(join(dir, "old.md"));
        } catch (ex) {
            if (!(ex instanceof Deno.errors.NotFound)) throw ex;
            oldExists = false;
        }
        assertEquals(oldExists, false, "the previous path must be removed");
        assertEquals(await Deno.readTextFile(join(dir, "new.md")), "moved text", "the destination keeps its content");
        await peer.stop();
    });
});

Deno.test("PeerStorage still blocks an empty payload that conflicts with its reported size", async () => {
    await withDir(async (dir) => {
        const { peer, notices, received } = makePeer(dir);
        const path = join(dir, "note.md");
        await seed(path, "previous content");
        const data: FileData = { ctime: DELIVERED_MTIME, mtime: DELIVERED_MTIME, size: 16, data: [""] };
        assertEquals(await peer.put("note.md", data), false, "the empty write is blocked");
        assertEquals(await Deno.readTextFile(path), "previous content", "the content is preserved");
        assertEquals(await mtimeOf(path), ON_DISK_MTIME, "the mtime is preserved");
        assertEquals(notices.filter((m) => m.startsWith("Empty write blocked")).length, 1, "a notice is given");
        assertEquals(received.filter((m) => m.includes("saved")).length, 0, "nothing is saved");
        await peer.stop();
    });
});

Deno.test("PeerStorage does not rewrite an empty file for an empty payload, but truncates a non-empty one", async () => {
    await withDir(async (dir) => {
        const { peer, received } = makePeer(dir);
        const empty = join(dir, "empty.md");
        await seed(empty, "");
        const emptyData: FileData = { ctime: DELIVERED_MTIME, mtime: DELIVERED_MTIME, size: 0, data: [""] };
        assertEquals(await peer.put("empty.md", emptyData), true, "an empty file stays in place");
        assertEquals(await mtimeOf(empty), ON_DISK_MTIME, "the empty file is not rewritten");
        assertEquals(received.filter((m) => m.includes("saved")).length, 0, "nothing is saved");

        const full = join(dir, "full.md");
        await seed(full, "content");
        assertEquals(await peer.put("full.md", emptyData), true, "an intentional empty edit is saved");
        assertEquals((await Deno.stat(full)).size, 0, "the file is emptied");
        await peer.stop();
    });
});
