import type { DirectFileManipulatorOptions } from "@vrtmrz/livesync-commonlib";

export interface Config {
    peers: PeerConf[];
    disableCheckingConfigMismatch?: boolean;
}
export type PeerConf = PeerStorageConf | PeerCouchDBConf;

export interface PeerPathIdentityConf {
    pathNormalizationVersion?: 0 | 1;
}

export interface PeerStorageConf extends PeerPathIdentityConf {
    scanOfflineChanges?: boolean;
    type: "storage";
    group?: string;
    name: string;
    baseDir: string;
    processor?: {
        cmd: string,
        args: string[]
    }
    useChokidar?: boolean;
    /** Glob patterns for paths which are never synchronised, e.g. [".git/**"] */
    ignore?: string[];
    handleFilenameCaseSensitive?: boolean;
    /**
     * Periodic rescan of the whole folder as a safety net for changes the watcher missed, in
     * minutes. The actual interval varies between 3/4 and 5/4 of this value. 0 disables it.
     * Default: 60. A rescan sends changed files only and never infers deletions.
     */
    rescanIntervalMinutes?: number;
    /** How many files are read and sent at the same time (default 10). */
    maxConcurrentSends?: number;
}
export interface PeerCouchDBConf extends DirectFileManipulatorOptions, PeerPathIdentityConf {
    /** Vault-root glob patterns for hidden i: files, selected in both directions, e.g. [".claude/**"]. */
    includeInternal?: string[];
    type: "couchdb";
    useRemoteTweaks?: true;
    group?: string;
    name: string;
    database: string;
    username: string;
    password: string;
    url: string;
    customChunkSize?: number;
    minimumChunkSize?: number;
    passphrase: string;
    obfuscatePassphrase: string;
    baseDir: string;
}



export function isCouchDBPeer(peer: PeerConf): peer is PeerCouchDBConf {
    return peer.type == "couchdb";
}

export function isStoragePeer(peer: PeerConf): peer is PeerStorageConf {
    return peer.type == "storage";
}

export type FileData = {
    ctime: number;
    mtime: number;
    size: number;
    data: string[] | Uint8Array<ArrayBuffer>;
    deleted?: boolean;
}
