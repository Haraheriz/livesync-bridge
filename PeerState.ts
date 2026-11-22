import { dirname, join } from "@std/path";
import { createSqliteFileValueStore, type SqliteValueStore } from "@vrtmrz/value-store/node-sqlite";
import { Logger, LOG_LEVEL_INFO, LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";

// Deno's localStorage is limited to 10 MB per origin. Storage peers keep one
// entry per file, so a vault with tens of thousands of files fills it and every
// later write throws QuotaExceededError. Peer state (offline-scan file records
// and CouchDB resume positions) is therefore kept in a SQLite file through
// @vrtmrz/value-store, which has no such limit.
//
// The store is asynchronous: call `initPeerState()` once at start-up, before any
// peer runs, and await every read and write.

export interface LegacyKeyValueSource {
    readonly length: number;
    key(index: number): string | null;
    getItem(key: string): string | null;
}

const IMPORT_MARKER = "imported-local-storage";
const IMPORT_CHUNK = 1000;
const WRITE_BATCH_SIZE = 1000;
const DEFERRED_DELAY_MS = 500;

interface PendingWrite {
    key: string;
    value: string;
    // Absent for a deferred write, whose failure is only logged.
    resolve?: () => void;
    reject?: (error: unknown) => void;
}

export interface PeerStateOptions {
    // Deferred records are committed once this many are queued (at most 1000 per transaction).
    batchSize?: number;
    // ... or after this long, whichever comes first.
    delayMs?: number;
    onDeferredError?: (error: unknown, count: number) => void;
}

// Docker images set DENO_DIR, and the Compose file persists
// $DENO_DIR/location_data, where localStorage used to live. Keep the state in
// that same directory so existing volumes and bind mounts continue to apply.
export function defaultStateFile(env: (key: string) => string | undefined = (key) => Deno.env.get(key)): string {
    const explicit = env("LSB_STATE_FILE");
    if (explicit) return explicit;
    const denoDir = env("DENO_DIR");
    if (denoDir) return join(denoDir, "location_data", "livesync-bridge-state.sqlite");
    return join("dat", "livesync-bridge-state.sqlite");
}

// ValueStoreError carries a stable `code` and no path or data; log that.
export function errorCode(ex: unknown): string {
    const code = (ex as { code?: unknown } | null)?.code;
    return typeof code === "string" ? code : `${ex}`;
}

export class PeerState {
    // Complete keys from Peer._getKey() are opaque here; no extra prefix is added.
    // The import marker lives in a second namespace so that clearing the state
    // never removes it.
    private readonly state: SqliteValueStore;
    private readonly meta: SqliteValueStore;
    constructor(readonly path: string, options: PeerStateOptions = {}) {
        this.batchSize = Math.min(options.batchSize ?? WRITE_BATCH_SIZE, WRITE_BATCH_SIZE);
        this.delayMs = options.delayMs ?? DEFERRED_DELAY_MS;
        this.onDeferredError = options.onDeferredError ?? ((error, count) =>
            Logger(`Could not save ${count} file records (they will be sent again by the next scan): ${errorCode(error)}`, LOG_LEVEL_NOTICE));
        Deno.mkdirSync(dirname(path), { recursive: true });
        this.state = createSqliteFileValueStore({ filePath: path, prefix: "", namespace: "peer-state" });
        this.meta = createSqliteFileValueStore({ filePath: path, prefix: "", namespace: "peer-state-meta" });
    }
    // Records that are queued or being committed, by key. Reads see them first,
    // so a caller never reads an older value than one it has already written.
    private queued = new Map<string, string>();
    private pending: PendingWrite[] = [];
    private timer: ReturnType<typeof setTimeout> | undefined;
    private flushing: Promise<void> = Promise.resolve();
    private draining = false;
    private readonly batchSize: number;
    private readonly delayMs: number;
    private readonly onDeferredError: (error: unknown, count: number) => void;
    getValue(key: string): Promise<string | null> {
        const queued = this.queued.get(key);
        if (queued !== undefined) return Promise.resolve(queued);
        return this.state.getValue(key);
    }
    // Writes issued in the same turn are committed together. An offline scan
    // saves one record per file, concurrently; committing each one alone is slow
    // (70,000 records took 145 s with the default FULL durability). The returned
    // Promise settles once the batch containing this write has committed or
    // failed, so a caller still sees its own failure. Use this for anything that
    // must not be lost, such as a CouchDB resume position.
    setValue(key: string, value: string): Promise<void> {
        return new Promise((resolve, reject) => {
            this.enqueue({ key, value, resolve, reject });
            this.schedule(0);
        });
    }
    // Queue a record without waiting for it to be saved. It is committed with
    // the next batch: after `batchSize` records, after `delayMs`, on flush(),
    // or on dispose(). A failure is logged once per batch, not thrown. Use this
    // only for records whose loss is harmless: a lost file record makes the next
    // offline scan send that file again, and nothing is skipped.
    setValueLater(key: string, value: string): void {
        this.enqueue({ key, value });
        this.schedule(this.pending.length >= this.batchSize ? 0 : this.delayMs);
    }
    private enqueue(write: PendingWrite) {
        this.pending.push(write);
        this.queued.set(write.key, write.value);
    }
    private schedule(delay: number) {
        // A full batch is written now, so a long burst cannot keep postponing the commit.
        if (this.pending.length >= this.batchSize) {
            if (!this.draining) void this.flush();
            return;
        }
        // A deferred request keeps a timer that is already waiting. A request for
        // "now" restarts it: a burst of writes in one turn then shares one commit.
        if (this.timer !== undefined) {
            if (delay > 0) return;
            clearTimeout(this.timer);
        }
        this.timer = setTimeout(() => {
            this.timer = undefined;
            void this.flush();
        }, delay);
        // A timer that only serves deferred records must not keep the process
        // alive; dispose() saves them. Awaited writes keep it referenced.
        if (delay > 0) Deno.unrefTimer(this.timer as unknown as number);
    }
    // Resolves once everything queued so far has been committed or has failed.
    // Never rejects: failures reach the writers (setValue) or the log (setValueLater).
    flush(): Promise<void> {
        if (this.timer !== undefined) {
            clearTimeout(this.timer);
            this.timer = undefined;
        }
        this.flushing = this.flushing.then(() => this.drain());
        return this.flushing;
    }
    private async drain(): Promise<void> {
        this.draining = true;
        try {
            await this.drainLoop();
        } finally {
            this.draining = false;
        }
    }
    private async drainLoop(): Promise<void> {
        while (this.pending.length > 0) {
            const batch = this.pending.splice(0, WRITE_BATCH_SIZE);
            let failure: unknown;
            let failed = false;
            try {
                await this.state.writeBatch(batch.map(({ key, value }) => ({ type: "setValue" as const, key, value })));
            } catch (ex) {
                failed = true;
                failure = ex;
            }
            let lost = 0;
            for (const write of batch) {
                // A later write to the same key keeps its own entry.
                if (this.queued.get(write.key) === write.value) this.queued.delete(write.key);
                if (!write.resolve) {
                    if (failed) lost++;
                } else if (failed) write.reject?.(failure);
                else write.resolve();
            }
            if (lost > 0) {
                try {
                    this.onDeferredError(failure, lost);
                } catch { /* logging must not stop the queue */ }
            }
        }
    }
    // Copy entries written by earlier versions once, so offline scans and CouchDB
    // checkpoints continue where they were. Entries already in the state file win,
    // and the source is left untouched. Returns the number of entries offered.
    async importLegacy(source: LegacyKeyValueSource | undefined): Promise<number> {
        if (!source) return 0;
        if (await this.meta.getValue(IMPORT_MARKER) !== null) return 0;
        const snapshot: [string, string][] = [];
        for (let i = 0; i < source.length; i++) {
            const key = source.key(i);
            if (key === null) continue;
            const value = source.getItem(key);
            if (value !== null) snapshot.push([key, value]);
        }
        // A failure here leaves the marker unset, so the next start retries safely.
        for (let i = 0; i < snapshot.length; i += IMPORT_CHUNK) {
            await this.state.writeBatch(snapshot.slice(i, i + IMPORT_CHUNK).map(([key, value]) => ({
                type: "setValue" as const,
                key,
                value,
                ifAbsent: true,
            })));
        }
        await this.meta.setValue(IMPORT_MARKER, "done");
        return snapshot.length;
    }
    // Forget all state. The marker is written first so that old localStorage
    // entries are never imported again afterwards. Stop all peers before calling.
    async reset(): Promise<void> {
        // Records queued before the reset must not reappear after it.
        await this.flush();
        await this.meta.setValue(IMPORT_MARKER, "reset");
        await this.state.clear();
    }
    async dispose(): Promise<void> {
        // Save what is queued; later writes are rejected by the store itself.
        await this.flush();
        try {
            await this.state.dispose();
        } finally {
            await this.meta.dispose();
        }
    }
}

let current: PeerState | undefined;

function legacyLocalStorage(): LegacyKeyValueSource | undefined {
    try {
        return globalThis.localStorage;
    } catch {
        // localStorage is unavailable when no storage location can be derived.
        return undefined;
    }
}

export interface InitPeerStateOptions {
    filePath?: string;
    // Clear the state and the legacy localStorage (the --reset flag).
    reset?: boolean;
    // Where to import earlier entries from. Defaults to the global localStorage.
    legacy?: LegacyKeyValueSource | undefined;
    // Tuning and error reporting for the write queue (mainly for tests).
    queue?: PeerStateOptions;
}

// Prepare the shared state. Await this before starting any peer.
export async function initPeerState(options: InitPeerStateOptions = {}): Promise<PeerState> {
    const state = new PeerState(options.filePath ?? defaultStateFile(), options.queue);
    const legacy = "legacy" in options ? options.legacy : legacyLocalStorage();
    try {
        if (options.reset) {
            await state.reset();
            // Clear the legacy source as well, so the next start has nothing to import.
            (legacy as Storage | undefined)?.clear?.();
        } else {
            const copied = await state.importLegacy(legacy);
            if (copied > 0) Logger(`Copied ${copied} entries from localStorage to ${state.path}`, LOG_LEVEL_INFO);
        }
    } catch (ex) {
        await state.dispose().catch(() => {});
        throw ex;
    }
    const previous = current;
    current = state;
    await previous?.dispose();
    return state;
}

export function getPeerState(): PeerState {
    if (!current) throw new Error("Peer state is not initialised: await initPeerState() before starting peers");
    return current;
}

// For tests: drop the shared state (and release its file).
export async function closePeerState(): Promise<void> {
    const state = current;
    current = undefined;
    await state?.dispose();
}
