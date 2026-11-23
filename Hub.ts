import { Config, FileData, isCouchDBPeer, isStoragePeer } from "./types.ts";
import { FileChangeContext, Peer, PeerHealth } from "./Peer.ts";
import { DEFAULT_EXCLUDE_NAMES, PeerStorage } from "./PeerStorage.ts";
import { Logger, LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";
import { PeerCouchDB } from "./PeerCouchDB.ts";
import { validatePeerPathIdentity } from "./pathIdentity.ts";

// A CouchDB peer's `includeInternal` selects hidden files (".claude/**"), but a storage peer of
// the same group leaves names starting with "." out in both directions by default
// (excludeDotNames), and the exclusion wins: those files are neither sent from nor written to
// that storage peer. Returns one notice per such pair, so that the two settings do not cancel
// each other silently.
export function internalFileExclusionNotices(conf: Config): string[] {
    const notices: string[] = [];
    for (const couchdb of conf.peers) {
        if (!isCouchDBPeer(couchdb) || (couchdb.includeInternal?.length ?? 0) === 0) continue;
        for (const storage of conf.peers) {
            if (!isStoragePeer(storage) || (storage.group ?? "") !== (couchdb.group ?? "")) continue;
            const dotNames = storage.excludeDotNames ?? true;
            const namedDots = (storage.excludeNames ?? DEFAULT_EXCLUDE_NAMES).filter((name) => name.startsWith("."));
            if (!dotNames && namedDots.length === 0) continue;
            const reason = dotNames
                ? `excludeDotNames is on (the default)`
                : `excludeNames holds ${namedDots.map((name) => `"${name}"`).join(", ")}`;
            notices.push(
                `Peer "${couchdb.name}" selects hidden files with includeInternal, but storage peer "${storage.name}" ` +
                    `leaves them out because ${reason}: they are neither sent from nor written to "${storage.name}". ` +
                    `The exclusion wins. To synchronise them, set "excludeDotNames": false` +
                    `${namedDots.length ? " and remove those names from excludeNames" : ""} on "${storage.name}".`,
            );
        }
    }
    return notices;
}

export class Hub {
    conf: Config;
    peers = [] as Peer[];
    constructor(conf: Config) {
        this.conf = conf;
    }
    // Aggregate peer health for the heartbeat. `ok` = every peer syncing (also
    // false if no peers were constructed). `restartWorthy` = any peer judges itself
    // restart-worthy (was healthy, now persistently failing while its backend is
    // up) — see Peer.probeHealth.
    async healthProbe(): Promise<{ ok: boolean; restartWorthy: boolean; peers: PeerHealth[] }> {
        const peers = await Promise.all(this.peers.map((p) => p.probeHealth()));
        const ok = peers.length > 0 && peers.every((p) => p.ok);
        const restartWorthy = peers.some((p) => p.restartWorthy);
        return { ok, restartWorthy, peers };
    }
    start() {
        validatePeerPathIdentity(this.conf);
        for (const notice of internalFileExclusionNotices(this.conf)) Logger(notice, LOG_LEVEL_NOTICE);
        for (const p of this.peers) {
            p.stop();
        }
        this.peers = [];
        for (const peer of this.conf.peers) {
            if (peer.type == "couchdb") {
                const p = new PeerCouchDB(peer, this.dispatch.bind(this), {
                    disableCheckingConfigMismatch: this.conf.disableCheckingConfigMismatch,
                    validateConfiguration: () => validatePeerPathIdentity(this.conf),
                });
                this.peers.push(p);
            } else if (peer.type == "storage") {
                const p = new PeerStorage(peer, this.dispatch.bind(this));
                this.peers.push(p);
            } else {
                throw new Error(`Unexpected Peer type: ${(peer as any)?.name} - ${(peer as any)?.type}`);
            }
        }
        // Initialize couchdb peers FIRST and await them, then start storage peers.
        // Otherwise a storage peer's offline scan can push to a couchdb peer before its
        // DB managers are initialized (initializeDatabase), causing
        // "Cannot read properties of undefined (reading 'getDBEntryMeta')".
        (async () => {
            for (const p of this.peers) {
                if (p.config.type === "couchdb") {
                    await p.start().catch((e) => {
                        console.error(`[Hub] peer "${p.config.name}" start() failed:`, e);
                    });
                }
            }
            for (const p of this.peers) {
                if (p.config.type !== "couchdb") {
                    p.start().catch((e) => {
                        console.error(`[Hub] peer "${p.config.name}" start() failed:`, e);
                    });
                }
            }
        })();
    }

    // Deliver to every other peer of the source's group. A peer which fails does not keep the
    // later peers from receiving the change: every peer is tried, and the failures are then
    // reported to the source together, so that it does not record the change as sent.
    // A single failure is rethrown as it is; several become one AggregateError.
    async dispatch(source: Peer, path: string, data: FileData | false, context?: FileChangeContext) {
        validatePeerPathIdentity(this.conf);
        const failures: unknown[] = [];
        for (const peer of this.peers) {
            if (peer !== source && (source.config.group ?? "") === (peer.config.group ?? "")) {
                let ret = false;
                try {
                    if (data === false) {
                        // Skip the deletion when the source still has the path (see Peer.deleteFrom).
                        ret = await peer.deleteFrom(path, source);
                    } else if (context?.previousPath !== undefined) {
                        ret = await peer.rename(context.previousPath, path, data);
                    } else {
                        ret = await peer.put(path, data);
                    }
                } catch (error) {
                    // Peers record repeats before writing. Do not acknowledge a failed write on retry.
                    peer.cache.clear();
                    failures.push(error);
                    continue;
                }
                if (ret) {
                    // Logger(`  ${data === false ? "-x->" : "--->"} ${peer.config.name} ${path} `)
                } else {
                    // Logger(`        ${peer.config.name} ignored ${path} `)
                }
            }
        }
        if (failures.length === 1) throw failures[0];
        if (failures.length > 1) {
            throw new AggregateError(failures, `Delivery of ${path} failed for ${failures.length} peers`);
        }
    }
}
