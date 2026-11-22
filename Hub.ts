import { Config, FileData } from "./types.ts";
import { FileChangeContext, Peer, PeerHealth } from "./Peer.ts";
import { PeerStorage } from "./PeerStorage.ts";
import { PeerCouchDB } from "./PeerCouchDB.ts";
import { validatePeerPathIdentity } from "./pathIdentity.ts";


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
