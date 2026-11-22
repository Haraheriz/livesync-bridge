// A save is reported as done only when the note and all its chunks are in CouchDB. The
// manipulator is replaced by a small in-memory CouchDB so the failures can be injected.
import type { DirectFileManipulator } from "@vrtmrz/livesync-commonlib";
import { PeerCouchDB } from "./PeerCouchDB.ts";
import type { FileData } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}

type Note = {
    _id: string;
    path: string;
    type: "plain";
    mtime: number;
    ctime: number;
    size: number;
    children: string[];
    eden?: Record<string, unknown>;
    deleted?: boolean;
};

class FakeCouch {
    notes = new Map<string, Note>();
    chunks = new Set<string>();
    // How the next put() stores the note: how many chunk IDs it refers to, and which of them
    // are actually written (a chunk write that fails in the background leaves a gap).
    chunkCount = 2;
    skipChunks: (id: string) => boolean = () => false;
    eden: Record<string, unknown> | undefined;
    putResult = true;
    puts = 0;
    allDocsCalls: string[][] = [];
    // Errors thrown by the next calls, to emulate CouchDB being down.
    rawGetFailure?: Error;
    allDocsFailure?: Error;
    // Called when the chunks are looked up, so a chunk can arrive in between.
    onAllDocs?: () => void;
    getByMetaFailure?: Error;
    existingContent?: string;

    man(): DirectFileManipulator {
        return {
            path2id: (path: string) => Promise.resolve(`id:${path}`),
            rawGet: (id: string) => {
                if (this.rawGetFailure) return Promise.reject(this.rawGetFailure);
                return Promise.resolve(this.notes.get(id) ?? false);
            },
            getById: (id: string) => {
                const note = this.notes.get(id);
                return note ? Promise.resolve(note) : Promise.reject(Object.assign(new Error("missing"), { status: 404 }));
            },
            getByMeta: (note: Note) => {
                if (this.getByMetaFailure) return Promise.reject(this.getByMetaFailure);
                return Promise.resolve({ ...note, data: [this.existingContent ?? ""] });
            },
            put: (path: string, _blob: Blob, info: { ctime: number; mtime: number; size: number }) => {
                this.puts++;
                if (!this.putResult) return Promise.resolve(false);
                const children = Array.from({ length: this.chunkCount }, (_, i) => `h:${path}:${i}`);
                for (const id of children) if (!this.skipChunks(id)) this.chunks.add(id);
                this.notes.set(`id:${path}`, {
                    _id: `id:${path}`,
                    path,
                    type: "plain",
                    ...info,
                    children,
                    eden: this.eden,
                });
                return Promise.resolve(true);
            },
            liveSyncLocalDB: {
                allDocsRaw: ({ keys }: { keys: string[] }) => {
                    this.allDocsCalls.push(keys);
                    if (this.allDocsFailure) return Promise.reject(this.allDocsFailure);
                    this.onAllDocs?.();
                    return Promise.resolve({
                        rows: keys.map((key) =>
                            this.chunks.has(key) ? { id: key, key, value: { rev: "1-a" } } : { key, error: "not_found" }
                        ),
                    });
                },
            },
        } as unknown as DirectFileManipulator;
    }
}

function makePeer(couch: FakeCouch) {
    const peer = new PeerCouchDB({
        type: "couchdb",
        name: `confirm-${crypto.randomUUID()}`,
        baseDir: "",
        url: "http://127.0.0.1:1",
        database: "unused",
        username: "",
        password: "",
        passphrase: "",
        obfuscatePassphrase: "",
    }, () => Promise.resolve());
    peer.normalLog = () => {};
    peer.receiveLog = () => {};
    peer.debugLog = () => {};
    peer.man = couch.man();
    (peer as unknown as { _started: { resolve(): void } })._started.resolve();
    // Look again straight away, so that the tests do not wait.
    peer.confirmIntervalMs = 0;
    return peer;
}

const file: FileData = { ctime: 1, mtime: 2, size: 5, data: ["hello"] };

async function failure(action: () => Promise<unknown>): Promise<string | undefined> {
    try {
        await action();
    } catch (error) {
        return error instanceof Error ? error.message : String(error);
    }
    return undefined;
}

Deno.test("a save whose note and chunks are all in CouchDB is confirmed", async () => {
    const couch = new FakeCouch();
    const peer = makePeer(couch);

    assertEquals(await peer.put("a.md", file), true, "saved");
    assertEquals(couch.allDocsCalls.length, 1, "the chunks were looked up once");
    assertEquals(couch.allDocsCalls[0].join(","), "h:a.md:0,h:a.md:1", "by their IDs, without loading the content");
});

Deno.test("a save whose chunk is missing is thrown, and is not remembered as handled", async () => {
    const couch = new FakeCouch();
    couch.skipChunks = (id) => id.endsWith(":1");
    const peer = makePeer(couch);

    const message = await failure(() => peer.put("m.md", file));
    assertEquals(message?.includes("could not be confirmed in CouchDB: m.md") ?? false, true, `thrown: ${message}`);
    assertEquals(message?.includes("1 of 2 chunk(s) not found") ?? false, true, `says what is missing: ${message}`);
    assertEquals(couch.allDocsCalls.length, peer.confirmAttempts, "the lookup was repeated before giving up");
    assertEquals(couch.puts, 1, "one save so far");

    // The same content is sent again, not taken for a repeat; this time the chunk is written.
    couch.skipChunks = () => false;
    assertEquals(await peer.put("m.md", file), true, "the retry saves");
    assertEquals(couch.puts, 2, "the retry really saved again");
});

Deno.test("a chunk which arrives while the note is being confirmed is accepted", async () => {
    const couch = new FakeCouch();
    couch.skipChunks = (id) => id.endsWith(":0");
    const peer = makePeer(couch);
    let lookups = 0;
    couch.onAllDocs = () => {
        if (++lookups === 2) couch.chunks.add("h:late.md:0");
    };

    assertEquals(await peer.put("late.md", file), true, "confirmed by the second lookup");
    assertEquals(couch.allDocsCalls.length, 2, "looked up twice");
});

Deno.test("a note which is not in CouchDB after saving is thrown", async () => {
    const couch = new FakeCouch();
    const peer = makePeer(couch);
    const man = peer.man as unknown as { put: (...args: unknown[]) => Promise<boolean> };
    man.put = () => {
        couch.puts++;
        return Promise.resolve(true); // reports success, but stores nothing
    };

    const message = await failure(() => peer.put("gone.md", file));
    assertEquals(message?.includes("the note is not found") ?? false, true, `thrown: ${message}`);
});

Deno.test("a note stored as deleted is not confirmed", async () => {
    const couch = new FakeCouch();
    const peer = makePeer(couch);
    const original = couch.man().put as unknown as (...args: unknown[]) => Promise<boolean>;
    const man = peer.man as unknown as { put: (...args: unknown[]) => Promise<boolean> };
    man.put = async (...args: unknown[]) => {
        const result = await original(...args);
        couch.notes.get("id:del.md")!.deleted = true;
        return result;
    };

    const message = await failure(() => peer.put("del.md", file));
    assertEquals(message?.includes("marked as deleted") ?? false, true, `thrown: ${message}`);
});

Deno.test("a CouchDB which cannot be asked is a failure, and the retry saves", async () => {
    const couch = new FakeCouch();
    const peer = makePeer(couch);

    couch.allDocsFailure = new Error("Connection refused");
    assertEquals(await failure(() => peer.put("d.md", file)), "Connection refused", "chunk lookup failure is thrown");
    couch.allDocsFailure = undefined;
    couch.rawGetFailure = new Error("Connection refused (note)");
    assertEquals(await failure(() => peer.put("d.md", file)), "Connection refused (note)", "note lookup failure is thrown");
    couch.rawGetFailure = undefined;

    assertEquals(await peer.put("d.md", file), true, "saved once CouchDB answers");
});

Deno.test("chunks embedded in the note are counted as present, and a note without chunks is fine", async () => {
    const couch = new FakeCouch();
    couch.skipChunks = () => true;
    couch.eden = { "h:e.md:0": { data: "x" }, "h:e.md:1": { data: "y" } };
    const peer = makePeer(couch);

    assertEquals(await peer.put("e.md", file), true, "embedded chunks need no lookup");
    assertEquals(couch.allDocsCalls.length, 0, "nothing was looked up");

    couch.eden = undefined;
    couch.chunkCount = 0;
    assertEquals(await peer.put("empty.md", { ...file, size: 0, data: [""] }), true, "a note without chunks is confirmed");
});

Deno.test("many chunks are looked up in batches", async () => {
    const couch = new FakeCouch();
    couch.chunkCount = 1200;
    const peer = makePeer(couch);

    assertEquals(await peer.put("big.md", file), true, "saved");
    assertEquals(couch.allDocsCalls.map((keys) => keys.length).join(","), "500,500,200", "500 IDs per request");
});

Deno.test("a stored copy which cannot be loaded is saved over instead of failing for ever", async () => {
    const couch = new FakeCouch();
    const peer = makePeer(couch);
    // CouchDB holds a note of the same time whose chunks are missing.
    couch.notes.set("id:old.md", {
        _id: "id:old.md",
        path: "old.md",
        type: "plain",
        ctime: 1,
        mtime: 2,
        size: 5,
        children: ["h:old.md:0"],
    });
    couch.getByMetaFailure = new Error("Load failed");

    assertEquals(await peer.put("old.md", file), true, "saved over the unloadable copy");
    assertEquals(couch.puts, 1, "one real save");
});
