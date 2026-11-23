// A note document can reach CouchDB before its chunks (an offline batch upload, or a replication that
// delivers the note first). The watching PeerCouchDB must then deliver nothing for that note until the
// chunks exist, and afterwards deliver exactly the original text. This file covers that with end-to-end
// encryption and path obfuscation switched on (and, in a second variant, encrypted metadata), because a
// chunk obtained while waiting for its arrival was once handed on still encrypted ("%=..." delivered as
// file content). PeerCouchDB.integration.ts covers the same retry only with encryption switched off.
//
// How the late chunks are produced: a writer PeerCouchDB stores notes in a source database. The test then
// copies documents into a second (destination) database by hand, keeping their revisions
// (`new_edits: false`): first everything except the chunks (`type === "leaf"`), later the chunks. The reader
// PeerCouchDB under test watches the destination database only. The encryption salt lives in the shared
// `_local/obsidian_livesync_sync_parameters` document, so it is copied before the reader starts.
//
// Run against a CouchDB (see test/integration/README.md):
//   COUCHDB_URL=http://127.0.0.1:5989 COUCHDB_USERNAME=admin COUCHDB_PASSWORD=testpassword \
//     deno test -A test/integration/LateChunksEncrypted.integration.ts
// Each test creates and deletes its own two databases (livesync_bridge_late_*), nothing else is touched.
import { MILESTONE_DOCID, TweakValuesTemplate, type FilePathWithPrefix } from "@vrtmrz/livesync-commonlib/compat/common/types";
import { getDocData } from "@vrtmrz/livesync-commonlib/compat/common/utils";
import { defaultLogger, setGlobalLogFunction } from "octagonal-wheels/common/logger";
import { PeerCouchDB } from "../../PeerCouchDB.ts";
import type { DispatchFun } from "../../Peer.ts";
import type { FileData, PeerCouchDBConf } from "../../types.ts";
import { installTempPeerState } from "../peerStateFixture.ts";

// Peers keep their state in SQLite; use a throw-away file for this run (as PeerCouchDB.integration.ts does).
await installTempPeerState();

const couchDbUrl = Deno.env.get("COUCHDB_URL") ?? "http://127.0.0.1:5989";
const couchDbUsername = Deno.env.get("COUCHDB_USERNAME") ?? "admin";
const couchDbPassword = Deno.env.get("COUCHDB_PASSWORD") ?? "testpassword";
const authHeader = `Basic ${btoa(`${couchDbUsername}:${couchDbPassword}`)}`;

const PARAMETERS_ID = "_local/obsidian_livesync_sync_parameters";
// How long "still nothing delivered" is observed. The watcher retries a failed load after 1 s, then 2 s, ...
const GRACE_MS = 2_500;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(message);
}

function head(text: string, length = 80): string {
    return JSON.stringify(text.length > length ? `${text.slice(0, length)}...` : text);
}

function assertSameText(actual: string, expected: string, message: string): void {
    if (actual === expected) return;
    let index = 0;
    while (index < actual.length && index < expected.length && actual[index] === expected[index]) index++;
    throw new Error(
        `${message}\nlength actual=${actual.length} expected=${expected.length}, first difference at ${index}\n` +
            `actual starts ${head(actual)}\nexpected starts ${head(expected)}`,
    );
}

// Text which looks like the output of the end-to-end encryption.
const looksEncrypted = (text: string) => /^%[=$~]/.test(text);

async function couch(path: string, method = "GET", body?: unknown): Promise<any> {
    const response = await fetch(`${couchDbUrl}/${path}`, {
        method,
        headers: { authorization: authHeader, "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    if (!response.ok) throw new Error(`${method} ${path}: HTTP ${response.status} ${text.slice(0, 300)}`);
    return text === "" ? undefined : JSON.parse(text);
}

async function waitForCouchDb(): Promise<void> {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
        try {
            const response = await fetch(`${couchDbUrl}/_up`, { headers: { authorization: authHeader } });
            await response.body?.cancel();
            if (response.ok) return;
        } catch {
            // CouchDB is still starting.
        }
        await sleep(500);
    }
    throw new Error(`CouchDB did not become ready at ${couchDbUrl} within 60 seconds`);
}

async function listIds(database: string): Promise<string[]> {
    const result = await couch(`${database}/_all_docs`);
    return (result.rows as { id: string }[]).map((row) => row.id).filter((id) => !id.startsWith("_design/"));
}

async function readDocs(database: string, ids: string[]): Promise<Record<string, any>[]> {
    if (ids.length === 0) return [];
    const result = await couch(`${database}/_all_docs?include_docs=true`, "POST", { keys: ids });
    return (result.rows as { key: string; doc?: Record<string, any>; error?: string }[]).map((row) => {
        assert(row.doc, `Document ${row.key} was not found in ${database} (${row.error ?? "no error"})`);
        return row.doc;
    });
}

// Copy documents as they are (same revisions) into another database.
async function copyDocs(source: string, destination: string, ids: string[]): Promise<void> {
    const docs = await readDocs(source, ids);
    if (docs.length === 0) return;
    const results = await couch(`${destination}/_bulk_docs`, "POST", { docs, new_edits: false });
    const failed = (results as { error?: string }[] | undefined)?.filter((result) => result.error) ?? [];
    assert(failed.length === 0, `Copying documents failed: ${JSON.stringify(failed).slice(0, 300)}`);
}

// Deterministic, varied text (words, numbers, hex, some Japanese) so that it splits into many chunks.
function variedText(seed: number, characters: number): string {
    let state = seed >>> 0;
    const random = () => (state = (Math.imul(state, 1664525) + 1013904223) >>> 0) / 2 ** 32;
    const words = ["alpha", "bridge", "chunk", "document", "encryption", "feed", "gamma", "hash", "index", "journal",
        "kernel", "ledger", "memo", "note", "orbit", "path", "queue", "retry", "sync", "vault", "日本語", "ノート", "同期", "暗号"];
    let out = "";
    for (let line = 0; out.length < characters; line++) {
        const parts = [`# ${line}`];
        for (let n = 8 + Math.floor(random() * 8); n > 0; n--) parts.push(words[Math.floor(random() * words.length)]);
        parts.push(Math.floor(random() * 2 ** 32).toString(16).padStart(8, "0"), Math.floor(random() * 2 ** 32).toString(16).padStart(8, "0"));
        out += `${parts.join(" ")}\n`;
    }
    return out;
}

type Variant = { label: string; encryptInternalMetadata: boolean };
const variants: Variant[] = [
    { label: "E2EE + path obfuscation", encryptInternalMetadata: false },
    { label: "E2EE + path obfuscation + encrypted metadata", encryptInternalMetadata: true },
];

type Delivery = { path: string; text: string; deleted: boolean };
type Stored = { path: string; content: string; noteIds: string[]; leafIds: string[]; leafData: string[] };

async function runScenario(variant: Variant, mode: "running" | "restart"): Promise<void> {
    await waitForCouchDb();
    const suffix = crypto.randomUUID().replaceAll("-", "");
    const short = suffix.slice(0, 8);
    const source = `livesync_bridge_late_src_${suffix}`;
    const destination = `livesync_bridge_late_dst_${suffix}`;
    const created: string[] = []; // only databases this test created are deleted
    const logs: string[] = [];
    const deliveries: Delivery[] = [];
    const attempted = new Set<string>(); // note documents the reader tried to load
    const failed = new Set<string>(); // ... of which the load failed
    const loaded: { id: string; text: string }[] = []; // what the reader's loads returned
    const settings = new Map<string, string>(); // the reader's saved state; shared by a restarted instance
    const writerSettings = new Map<string, string>();
    const readerName = `late-reader-${suffix}`;

    const configFor = (name: string, database: string): PeerCouchDBConf => ({
        type: "couchdb",
        name,
        database,
        username: couchDbUsername,
        password: couchDbPassword,
        url: couchDbUrl,
        passphrase: "content-encryption-passphrase",
        obfuscatePassphrase: "path-obfuscation-passphrase",
        baseDir: "",
        encryptInternalMetadata: variant.encryptInternalMetadata,
    });
    const keepState = (peer: PeerCouchDB, store: Map<string, string>) => {
        peer.getSetting = (key) => Promise.resolve(store.get(key) ?? null);
        peer.setSetting = (key, value) => {
            store.set(key, value);
            return Promise.resolve();
        };
    };
    const dispatcher: DispatchFun = (_source, path, data: FileData | false) => {
        if (data === false) deliveries.push({ path, text: "", deleted: true });
        else {
            deliveries.push({
                path,
                text: data.data instanceof Uint8Array ? new TextDecoder().decode(data.data) : getDocData(data.data),
                deleted: false,
            });
        }
        return Promise.resolve();
    };
    // Record every load the reader makes (the watcher's own retries go through getByMeta).
    const observeLoads = (peer: PeerCouchDB) => {
        const original = peer.man.getByMeta.bind(peer.man);
        peer.man.getByMeta = async (doc) => {
            attempted.add(doc._id);
            try {
                const result = await original(doc);
                if ("data" in result && result.type === "plain") loaded.push({ id: doc._id, text: getDocData(result.data) });
                return result;
            } catch (error) {
                failed.add(doc._id);
                throw error;
            }
        };
    };

    const writer = new PeerCouchDB(configFor(`late-writer-${suffix}`, source), () => Promise.resolve());
    keepState(writer, writerSettings);
    const newReader = () => {
        const reader = new PeerCouchDB({
            ...configFor(readerName, destination),
            ...(variant.encryptInternalMetadata ? { useRemoteTweaks: true as const } : {}),
        }, dispatcher);
        keepState(reader, settings);
        return reader;
    };
    let reader = newReader();
    const readers: PeerCouchDB[] = [reader];

    const diagnostics = () => {
        const relevant = logs.filter((line) =>
            /WATCH|LOAD FAILED|Corrupted|chunk|retry|Empty update|change detected|Remote database|Watch starting|\bfailed\b/i.test(line)
        ).slice(-60);
        return [
            `--- diagnostics (${variant.label}, ${mode}) ---`,
            `deliveries (${deliveries.length}):`,
            ...deliveries.map((d, i) => `  #${i} ${head(d.path)} ${d.deleted ? "DELETED" : `${d.text.length} chars ${head(d.text)}`}`),
            `loads attempted=${attempted.size} failed=${failed.size}; returned plaintext heads: ${
                JSON.stringify(loaded.map((l) => l.text.slice(0, 80)))
            }`,
            `reader state keys: ${[...settings.keys()].map((key) => key.slice(0, 40)).join(", ")}`,
            `relevant log lines (last ${relevant.length}):`,
            ...relevant.map((line) => `  ${line.slice(0, 200)}`),
        ].join("\n");
    };
    const waitFor = async (condition: () => boolean, timeoutMs: number, what: string) => {
        const deadline = Date.now() + timeoutMs;
        while (Date.now() < deadline) {
            if (condition()) return;
            await sleep(25);
        }
        throw new Error(`Timed out after ${timeoutMs} ms waiting for: ${what}`);
    };
    // For observations which are timing dependent but not required for correctness.
    const waitSoft = async (condition: () => boolean, timeoutMs: number) => {
        const deadline = Date.now() + timeoutMs;
        while (Date.now() < deadline && !condition()) await sleep(25);
        return condition();
    };
    const deliveriesFor = (path: string) => deliveries.filter((d) => d.path === path);
    const assertNothingDelivered = (paths: string[], when: string) => {
        for (const path of paths) {
            assert(deliveriesFor(path).length === 0, `${path} must not be delivered ${when}; got ${JSON.stringify(deliveriesFor(path).map((d) => head(d.text)))}`);
        }
    };
    // Every delivery ever observed for the path must be the exact plaintext (not only the last one).
    const assertDelivered = (stored: Stored) => {
        const seen = deliveriesFor(stored.path);
        assert(seen.length > 0, `${stored.path} was not delivered`);
        seen.forEach((delivery, index) => {
            assert(!delivery.deleted, `${stored.path} delivery #${index} is a deletion`);
            assert(!looksEncrypted(delivery.text), `${stored.path} delivery #${index} starts with ciphertext: ${head(delivery.text)}`);
            for (const cipher of stored.leafData) {
                assert(!delivery.text.includes(cipher.slice(0, 40)), `${stored.path} delivery #${index} contains chunk ciphertext`);
            }
            assertSameText(delivery.text, stored.content, `${stored.path} delivery #${index} differs from the original`);
        });
    };
    const writeNote = async (path: string, content: string, minimumChunks: number): Promise<Stored> => {
        const before = new Set(await listIds(source));
        const data: FileData = { ctime: 1_700_000_000_000, mtime: 1_700_000_000_000, size: new TextEncoder().encode(content).byteLength, data: [content] };
        assert(await writer.put(path, data), `The writer should store ${path}`);
        const added = (await listIds(source)).filter((id) => !before.has(id));
        const docs = await readDocs(source, added);
        const leaves = docs.filter((doc) => doc.type === "leaf");
        const notes = docs.filter((doc) => doc.type !== "leaf");
        // Fixture preconditions: a failure here means the fixture does not exercise what this test is about.
        assert(notes.length === 1, `Fixture: storing ${path} should add exactly one note document, got ${notes.length}`);
        assert(leaves.length >= minimumChunks, `Fixture: ${path} should have at least ${minimumChunks} separate chunk document(s), got ${leaves.length}`);
        const raw = notes[0];
        assert(raw.path !== path && !String(raw._id).includes(path), `Fixture: the note document of ${path} should have an obfuscated path and ID`);
        const leafData = leaves.map((leaf) => String(leaf.data));
        for (const cipher of leafData) {
            assert(!cipher.includes(content.slice(0, 20)), `Fixture: a chunk of ${path} is not encrypted`);
        }
        return { path, content, noteIds: notes.map((n) => n._id), leafIds: leaves.map((l) => l._id), leafData };
    };

    try {
        for (const database of [source, destination]) {
            await couch(database, "PUT");
            created.push(database);
        }
        setGlobalLogFunction((message) => logs.push(String(message)));

        // The writer; its first note creates the shared encryption parameters, which the reader must share.
        await writer.start();
        const warmupBefore = await listIds(source);
        assert(warmupBefore.length === 0, "Fixture: the source database must start empty");
        await writer.put(`warmup-${short}.md`, { ctime: 1, mtime: 1, size: 6, data: ["warmup"] });
        const skipped = new Set(await listIds(source)); // never copied: the reader must not see the warm-up note
        const parameters = await couch(`${source}/${PARAMETERS_ID}`);
        delete parameters._rev;
        await couch(`${destination}/${PARAMETERS_ID}`, "PUT", parameters);
        // Without this document the reader would treat the destination as an empty remote and not watch it.
        await couch(`${destination}/${MILESTONE_DOCID}`, "PUT", {
            created: Date.now(),
            type: "milestoneinfo",
            accepted_nodes: [],
            node_chunk_info: {},
            ...(variant.encryptInternalMetadata
                ? {
                    tweak_values: {
                        PREFERRED: {
                            ...TweakValuesTemplate,
                            encrypt: true,
                            usePathObfuscation: true,
                            encryptInternalMetadata: true,
                        },
                    },
                }
                : {}),
        });

        const tiny = await writeNote(`late-tiny-${short}.md` as FilePathWithPrefix, `tiny note ${suffix}`, 1);
        const big = await writeNote(`late-big-${short}.md` as FilePathWithPrefix, variedText(parseInt(short, 16), 48_000), 3);
        const waiting = [tiny, big];
        const waitingPaths = waiting.map((stored) => stored.path);
        const copiedToDestination = new Set<string>(skipped);
        const copy = async (ids: string[]) => {
            const fresh = ids.filter((id) => !copiedToDestination.has(id));
            await copyDocs(source, destination, fresh);
            fresh.forEach((id) => copiedToDestination.add(id));
        };

        await reader.start();
        assert(reader.man.liveSyncLocalDB.isReady, "The reader's direct database should be ready");
        observeLoads(reader);

        // Only the note documents reach the destination; their chunks are still missing.
        await copy(waiting.flatMap((stored) => stored.noteIds));
        const destinationDocs = await readDocs(destination, (await listIds(destination)));
        assert(!destinationDocs.some((doc) => doc.type === "leaf"), "Fixture: no chunk may be in the destination yet");
        await waitSoft(() => waiting.every((stored) => stored.noteIds.every((id) => attempted.has(id))), 10_000);
        await waitSoft(() => waiting.every((stored) => stored.noteIds.every((id) => failed.has(id))), 10_000);

        // (a) nothing is delivered while only the note documents are present
        await sleep(GRACE_MS);
        assertNothingDelivered(waitingPaths, "while its chunks are missing");

        if (mode === "running") {
            // Some of the big note's chunks arrive; the note is still incomplete. (The tiny note's chunk is withheld.)
            const some = big.leafIds.slice(0, Math.floor(big.leafIds.length / 2));
            assert(some.length >= 1 && some.length < big.leafIds.length, "Fixture: the big note should be split into at least 2 chunks");
            await copy(some);
            await sleep(GRACE_MS);
            assertNothingDelivered(waitingPaths, "while only some of its chunks have arrived");

            // (c) an unrelated note (note and chunks complete) is delivered while the first two keep waiting
            const other = await writeNote(`late-other-${short}.md`, `unrelated note ${suffix}\n${variedText(7, 300)}`, 1);
            await copy([...other.noteIds, ...other.leafIds]);
            await waitFor(() => deliveriesFor(other.path).length > 0, 30_000, "delivery of the unrelated note");
            assertDelivered(other);
            assertNothingDelivered(waitingPaths, "while only the unrelated note's chunks arrived");
            await sleep(GRACE_MS);
            assertNothingDelivered(waitingPaths, "after the unrelated note was delivered");
        } else {
            // (d) the reader restarts while both notes wait
            await reader.stop();
            await reader.man.close();
            assertNothingDelivered(waitingPaths, "before the restart");
            attempted.clear();
            failed.clear();
            reader = newReader();
            readers.push(reader);
            await reader.start();
            observeLoads(reader);
            await waitSoft(() => waiting.every((stored) => stored.noteIds.every((id) => attempted.has(id))), 10_000);
            assertNothingDelivered(waitingPaths, "right after the restart");

            // (c) an unrelated complete note is still delivered by the restarted reader
            const other = await writeNote(`late-other-${short}.md`, `unrelated note ${suffix}\n${variedText(7, 300)}`, 1);
            await copy([...other.noteIds, ...other.leafIds]);
            await waitFor(() => deliveriesFor(other.path).length > 0, 30_000, "delivery of the unrelated note after the restart");
            assertDelivered(other);
            assertNothingDelivered(waitingPaths, "after the restart while its chunks are missing");
            await sleep(GRACE_MS);
            assertNothingDelivered(waitingPaths, "after the restart, a while later");
        }

        // The remaining chunks arrive (the copy skips those already copied).
        await copy(waiting.flatMap((stored) => stored.leafIds));
        for (const stored of waiting) {
            await waitFor(() => deliveriesFor(stored.path).length > 0, 90_000, `delivery of ${stored.path} after its chunks arrived`);
        }
        // (b) the exact plaintext, for every delivery; checked again after a pause to catch a later bad redelivery
        for (const stored of waiting) assertDelivered(stored);
        await sleep(GRACE_MS);
        for (const stored of waiting) assertDelivered(stored);
        assert(deliveries.every((d) => !looksEncrypted(d.text)), "No delivery at all may start with ciphertext");
        // What the reader loaded must never have been ciphertext either (the chunk path, not only the delivery).
        const cipherLoads = loaded.filter((load) => looksEncrypted(load.text));
        assert(cipherLoads.length === 0, `A load returned ciphertext: ${head(cipherLoads[0]?.text ?? "")}`);
    } catch (error) {
        console.log(diagnostics());
        throw error;
    } finally {
        for (const peer of [writer, ...readers]) await Promise.allSettled([peer.stop()]);
        await Promise.allSettled([writer.man?.close(), ...readers.map((peer) => peer.man?.close())]);
        setGlobalLogFunction(defaultLogger);
        for (const database of created) await Promise.allSettled([couch(database, "DELETE")]);
    }
}

for (const variant of variants) {
    // PeerCouchDB.stop() only ends the watch and the manipulator keeps its own timers and connections until
    // closed, so the leak sanitizers are disabled (as the other integration suites that restart peers do).
    Deno.test({
        name: `Late chunks | ${variant.label} | running reader delivers the exact text once the chunks arrive`,
        sanitizeOps: false,
        sanitizeResources: false,
        fn: () => runScenario(variant, "running"),
    });
    Deno.test({
        name: `Late chunks | ${variant.label} | restarted reader delivers the exact text once the chunks arrive`,
        sanitizeOps: false,
        sanitizeResources: false,
        fn: () => runScenario(variant, "restart"),
    });
}
