import { DirectFileManipulator } from "@vrtmrz/livesync-commonlib";
import type { DispatchFun } from "./Peer.ts";
import { PeerCouchDB } from "./PeerCouchDB.ts";
import type { PeerCouchDBConf } from "./types.ts";

function assertEquals(actual: unknown, expected: unknown, message: string) {
    if (actual !== expected) throw new Error(`${message}: expected=${expected}, actual=${actual}`);
}

const activeWatches = new Set<DirectFileManipulator>();
function checkpointTest(name: string, run: () => void | Promise<void>) {
    Deno.test(name, async () => {
        try { await run(); }
        finally {
            for (const man of activeWatches) man.endWatch();
            activeWatches.clear();
        }
    });
}

async function makePeer(settings = new Map<string, string>(), overrides: Partial<PeerCouchDBConf> = {}, dispatcher: DispatchFun = async () => {}) {
    const peer = new PeerCouchDB({
        type: "couchdb", name: "checkpoint-peer", baseDir: "",
        url: "https://one.example.test/proxy", database: "notes", username: "", password: "",
        passphrase: "", obfuscatePassphrase: "", ...overrides,
    }, dispatcher);
    peer.setSetting = (key, value) => { settings.set(peer._getKey(key), value); return Promise.resolve(); };
    peer.getSetting = (key) => Promise.resolve(settings.get(peer._getKey(key)) ?? null);
    peer.normalLog = () => {};
    peer.sendLog = () => {};
    peer.man = { since: await peer.getSetting("since") ?? "" } as DirectFileManipulator;
    return peer;
}

for (const [description, overrides] of [
    ["server", { url: "https://two.example.test/proxy" }],
    ["database", { database: "other-notes" }],
    ["peer", { name: "another-peer" }],
    ["base directory", { baseDir: "subfolder/" }],
    ["proxy path", { url: "https://one.example.test/other-proxy" }],
    ["path identity", { pathNormalizationVersion: 1 }],
] as const) {
    checkpointTest(`CouchDB checkpoints are isolated by ${description}`, async () => {
        const settings = new Map<string, string>();
        const first = await makePeer(settings);
        await first.advanceSince("41-first");
        await first.setSetting("remote-created", "7");
        const other = await makePeer(settings, overrides);
        assertEquals(await other.getSetting("since"), null, "A different scope must not inherit the previous sequence");
        assertEquals(await other.getSetting("remote-created"), null, "The remote generation must have the same scope");
        await other.advanceSince("9-other");
        await other.setSetting("remote-created", "7");
        const restored = await makePeer(settings);
        assertEquals(await restored.getSetting("since"), "41-first", "Returning to the original remote must restore its sequence");
        assertEquals(await restored.getSetting("remote-created"), "7", "The original remote generation must remain available");
    });
}

checkpointTest("CouchDB checkpoint identity normalises endpoints and excludes credentials", async () => {
    const settings = new Map<string, string>();
    const first = await makePeer(settings, { url: "https://alice:old-secret@ONE.example.test:443/proxy/", password: "old-password" });
    await first.advanceSince("41-token");
    await first.setSetting("remote-created", "7");
    const restored = await makePeer(settings, { url: "https://bob:new-secret@one.example.test/proxy", password: "new-password" });
    assertEquals(await restored.getSetting("since"), "41-token", "Credential rotation and equivalent URLs must preserve the sequence");
    assertEquals(await restored.getSetting("remote-created"), "7", "Equivalent endpoints must preserve the generation");
    assertEquals([...settings.keys()].some((key) => /alice|bob|secret|password/.test(key)), false, "Checkpoint keys must not contain credentials");
});

checkpointTest("CouchDB checkpoint scopes cannot collide through separator characters", async () => {
    const settings = new Map<string, string>();
    const first = await makePeer(settings, { name: "alpha-couchdb-beta", baseDir: "gamma" });
    const other = await makePeer(settings, { name: "alpha", baseDir: "beta-couchdb-gamma" });
    await first.advanceSince("41-token");
    assertEquals(await other.getSetting("since"), null, "Different peer scopes must remain separate even when legacy keys collide");
});

checkpointTest("CouchDB checkpoint scoping preserves other peer setting keys", async () => {
    const peer = await makePeer();
    assertEquals(peer._getKey("other-setting"), "checkpoint-peer-couchdb--other-setting", "Unrelated settings must retain their existing keys");
});

type Change = {
    seq: string;
    doc: { _id: string; path: string; type: string; data: string[]; size: number; ctime: number; mtime: number; deleted?: boolean };
};

async function attachWatch(peer: PeerCouchDB, created: number | false = 7) {
    let onChange!: (event: Change) => Promise<void>;
    let startingSince: string | undefined;
    const feed = {
        cancel() {},
        on(event: string, handler: unknown) {
            if (event === "change") onChange = handler as typeof onChange;
            return feed;
        },
    };
    peer.man = {
        since: await peer.getSetting("since") ?? "",
        watching: false,
        ready: { promise: Promise.resolve() },
        rawGet: () => Promise.resolve(created === false ? false : { created }),
        getByMeta: (doc: Change["doc"]) => Promise.resolve(doc),
        beginWatch: DirectFileManipulator.prototype.beginWatch,
        endWatch: DirectFileManipulator.prototype.endWatch,
        liveSyncLocalDB: { localDatabase: { changes(options: { since: string }) { startingSince = options.since; return feed; } } },
    } as unknown as DirectFileManipulator;
    activeWatches.add(peer.man);
    await (peer as unknown as { _connectAndWatch(): Promise<void> })._connectAndWatch();
    return {
        startingSince,
        emit: async (seq: string, deleted = false, path = "note.md") => {
            await onChange({ seq, doc: {
                _id: path, path, type: "plain", data: ["note"], size: 4, ctime: 1, mtime: 2, deleted,
            } });
            await new Promise((resolve) => setTimeout(resolve, 0));
        },
    };
}

checkpointTest("CouchDB does not reuse legacy checkpoints without a remote identity", async () => {
    const settings = new Map([
        ["checkpoint-peer-couchdb--since", "999-old"],
        ["checkpoint-peer-couchdb--remote-created", "7"],
    ]);
    const peer = await makePeer(settings);
    assertEquals(await peer.getSetting("since"), null, "An unbound legacy sequence must not be trusted");
    const watch = await attachWatch(peer);
    assertEquals(watch.startingSince, "", "An unrecognised remote must be read from the beginning");
    assertEquals(await peer.getSetting("since"), "", "The initial position must be persisted before any change arrives");
    assertEquals(settings.get("checkpoint-peer-couchdb--since"), "999-old", "Legacy state must be left untouched");
});

checkpointTest("CouchDB persists a rebuilt database's reset before starting its feed", async () => {
    const settings = new Map<string, string>();
    const peer = await makePeer(settings);
    await peer.setSetting("since", "100-old-database");
    await peer.setSetting("remote-created", "7");
    const watch = await attachWatch(peer, 8);
    assertEquals(watch.startingSince, "", "A new generation must start from the beginning");
    assertEquals(await (await makePeer(settings)).getSetting("since"), "", "A restart before the first change must not restore the old sequence");
    assertEquals(await peer.getSetting("remote-created"), "8", "The new generation must be recorded");
});

checkpointTest("CouchDB clears an old checkpoint when the remote is empty", async () => {
    const peer = await makePeer();
    await peer.setSetting("since", "100-old-database");
    await peer.setSetting("remote-created", "7");
    await attachWatch(peer, false);
    assertEquals(peer.man.since, "", "An empty remote must discard the old in-memory sequence");
    assertEquals(await peer.getSetting("since"), "", "An empty remote must discard the old saved sequence");
    assertEquals(await peer.getSetting("remote-created"), "0", "The empty remote marker must remain scoped to this peer");
});

checkpointTest("CouchDB persists the feed sequence after dispatch and resumes a new peer from it", async () => {
    const settings = new Map<string, string>();
    const started = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const peer = await makePeer(settings, {}, async () => { started.resolve(); await release.promise; });
    await peer.setSetting("since", "40-token");
    await peer.setSetting("remote-created", "7");
    const watch = await attachWatch(peer);
    const pending = watch.emit("41-token");
    await started.promise;
    try {
        assertEquals(await peer.getSetting("since"), "40-token", "A pending dispatch must not advance its own checkpoint");
    } finally {
        release.resolve();
        await pending;
    }
    assertEquals(peer.man.since, "41-token", "A reconnect must use the delivered change's position");
    const restarted = await makePeer(settings);
    assertEquals((await attachWatch(restarted)).startingSince, "41-token", "A new peer instance must resume from the saved position");
});

checkpointTest("CouchDB does not advance the checkpoint of a rejected dispatch", async () => {
    const peer = await makePeer(undefined, {}, () => Promise.reject(new Error("Controlled destination failure")));
    await peer.setSetting("since", "40-token");
    await peer.setSetting("remote-created", "7");
    const watch = await attachWatch(peer);
    await watch.emit("41-token");
    assertEquals(await peer.getSetting("since"), "40-token", "A rejected dispatch must retain its own checkpoint");
});

checkpointTest("CouchDB checkpoints a dispatched deletion", async () => {
    let deletion = false;
    const peer = await makePeer(undefined, {}, (_source, _path, data) => { deletion = data === false; return Promise.resolve(); });
    await peer.setSetting("remote-created", "7");
    const watch = await attachWatch(peer);
    await watch.emit("41-token", true);
    assertEquals(deletion, true, "The hub must receive the deletion");
    assertEquals(await peer.getSetting("since"), "41-token", "A delivered deletion must advance the checkpoint");
});

checkpointTest("CouchDB retains a failed load when a later file is delivered", async () => {
    const delivered: string[] = [];
    const peer = await makePeer(undefined, {}, (_source, path) => {
        delivered.push(path);
        return Promise.resolve();
    });
    await peer.setSetting("since", "40-token");
    await peer.setSetting("remote-created", "7");
    const watch = await attachWatch(peer);
    const read = peer.man.getByMeta.bind(peer.man);
    peer.man.getByMeta = (doc) => doc.path === "late.md"
        ? Promise.reject(new Error("Controlled missing chunk"))
        : read(doc);
    await watch.emit("41-token", false, "late.md");
    peer.man.getByMeta = read;
    await watch.emit("42-token", false, "later.md");
    assertEquals(delivered.length, 1, "The later readable file must still be delivered");
    assertEquals(await peer.getSetting("since"), "40-token", "A later delivery must not checkpoint past a failed load");
});

checkpointTest("NFC checkpoint identity includes the configured filename case rule", async () => {
    const settings = new Map<string, string>();
    const sensitive = await makePeer(settings, { pathNormalizationVersion: 1, handleFilenameCaseSensitive: true });
    await sensitive.advanceSince("41-sensitive");
    await sensitive.setSetting("remote-created", "7");
    const insensitive = await makePeer(settings, { pathNormalizationVersion: 1, handleFilenameCaseSensitive: false });
    assertEquals(await insensitive.getSetting("since"), null, "Another NFC case rule must not reuse the sequence");
    const restored = await makePeer(settings, { pathNormalizationVersion: 1, handleFilenameCaseSensitive: true });
    assertEquals(await restored.getSetting("since"), "41-sensitive", "The matching NFC policy must restore its sequence");
});
