import { PeerStorageConf, FileData } from "./types.ts";
import { delay, getDocData } from "@vrtmrz/livesync-commonlib/compat/common/utils";
import { isPlainText } from "@vrtmrz/livesync-commonlib/compat/string_and_binary/path";
import { parse, format, relative, dirname, resolve, SEPARATOR, isAbsolute } from "@std/path";
import { format as posixFormat, parse as posixParse } from "@std/path/posix";
import { serialized, withLock } from "octagonal-wheels/concurrency/lock";
import { Semaphore } from "octagonal-wheels/concurrency/semaphore_v2";
import { DispatchFun, FileChangeContext, Peer, PeerHealth } from "./Peer.ts";
import chokidar from "chokidar";
import { minimatch } from "minimatch";
import { parseFilePath, type PathComparisonSettings, type StoragePath } from "@vrtmrz/livesync-commonlib/storage";
import { DenoStoragePathAccess } from "./DenoStoragePathAccess.ts";
import { getPeerPathNormalizationVersion } from "./pathIdentity.ts";
import { errorCode, getPeerState } from "./PeerState.ts";

import {
    Logger,
    LOG_LEVEL_INFO,
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_VERBOSE,
} from "octagonal-wheels/common/logger";

// Ignore patterns are matched with minimatch, the same matcher the CouchDB peer uses
// for `includeInternal`. `dot` is needed because the directories which are worth
// ignoring at all are usually dot-directories, such as `.git`.
const IGNORE_MATCH_OPTIONS = { dot: true } as const;

// How many files are read and sent at the same time. LiveSync itself limits its own bulk
// processing to 10 (ReplicateResultProcessor, HiddenFileSync: Semaphore(10)).
export const DEFAULT_MAX_CONCURRENT_SENDS = 10;
// Folders a Synology NAS creates inside a shared folder by itself: thumbnails, the recycle bin
// and the snapshot view. Synced, they would be written into every device's vault.
export const DEFAULT_EXCLUDE_NAMES = ["@eaDir", "#recycle", "#snapshot"];
const SEND_PROGRESS_INTERVAL_MS = 60_000;
// Delay before rescanning a folder Chokidar reported as added (see onAddDir).
export const NEW_DIR_RESCAN_DELAY_MS = 2_000;
export const DEFAULT_RESCAN_INTERVAL_MINUTES = 60;
// A send that failed is tried again 30, 60, 90, 120 and 150 seconds later (n times the interval).
export const DEFAULT_SEND_RETRY_INTERVAL_SECONDS = 30;
export const MAX_SEND_RETRIES = 5;
const MAX_GIVE_UP_NOTICES = 20;

// Byte-for-byte equality of two buffers.
function isSameBytes(a: Uint8Array, b: Uint8Array): boolean {
    if (a.byteLength !== b.byteLength) return false;
    for (let i = 0; i < a.byteLength; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}

// A short reason for a failure, for notices: the OS error code when there is one.
function failureReason(ex: unknown): string {
    if (!(ex instanceof Error)) return "not readable";
    const code = (ex as { code?: unknown }).code;
    return typeof code === "string" ? code : ex.name;
}

export class PeerStorage extends Peer {
    declare config: PeerStorageConf;
    // The platform's path separator; a field so tests can simulate Windows on any OS.
    pathSeparator: string = SEPARATOR;
    // Obsidian's Vault supplies per-file delete events when a folder is removed, and
    // Self-hosted LiveSync ignores folder events. Deno.watchFs may report only the
    // removed parent, so retain file paths to emulate the missing child events.
    // The children cannot be enumerated from disk after their folder has gone.
    private knownFiles = new Set<string>();
    private readyDenoWatcher?: Deno.FsWatcher;
    private readyChokidarWatcher?: chokidar.FSWatcher;
    // Lifecycle. stop() ends the admission of new work and waits for what was accepted.
    private stopped = false;
    private generation = 0;
    private closing?: Promise<void>;
    private pendingEvents = new Map<string, Deno.FsEvent>();
    private pendingTasks = new Map<string, { next?: () => Promise<void>; promise: Promise<void> }>();
    private rescanTimer?: ReturnType<typeof setTimeout>;
    private newDirTimers = new Map<string, ReturnType<typeof setTimeout>>();
    private fullScanRunning = false;
    private watcherReady = false;
    // Set while Deno.watchFs is not running (it could not be started, or it failed): the peer then
    // lives on its periodic rescans, which also try to start the watcher again.
    private watchDegraded = false;
    // The inventory taken at start without a watcher is done: rescans may begin.
    private degradedScanDone = false;
    private lastWatchProblem = "";
    private backgroundWatch?: Promise<void>;
    // Reads one directory. A field so that tests can make a folder unreadable on any OS.
    readDirectory: (path: string) => AsyncIterable<Deno.DirEntry> = (path) => Deno.readDir(path);

    // Run one file's work at a time. A request that arrives while the same key is
    // waiting replaces the waiting one, so a burst of events reads the file once.
    private queueTask(key: string, lock: string, action: () => Promise<void>): Promise<void> {
        const previous = this.pendingTasks.get(key);
        if (previous) {
            previous.next = action;
            return previous.promise;
        }
        const pending = { next: action as (() => Promise<void>) | undefined, promise: Promise.resolve() };
        this.pendingTasks.set(key, pending);
        pending.promise = serialized(lock, async () => {
            const failures: unknown[] = [];
            while (pending.next) {
                const next = pending.next;
                pending.next = undefined;
                try {
                    await next();
                } catch (ex) {
                    failures.push(ex);
                }
            }
            if (failures.length) throw new AggregateError(failures, `File change failed: ${key}`);
        }).finally(() => {
            this.pendingTasks.delete(key);
        });
        // Watcher callbacks have no caller to receive rejection. Keep failures visible
        // without acknowledging their contents or losing the drainable promise.
        void pending.promise.catch((ex) => {
            this.normalLog(`File change failed: ${key}`, LOG_LEVEL_NOTICE);
            Logger(ex, LOG_LEVEL_VERBOSE);
        });
        return pending.promise;
    }

    private async drain(): Promise<void> {
        while (this.pendingTasks.size) {
            await Promise.allSettled([...this.pendingTasks.values()].map((task) => task.promise));
        }
    }

    // ---- Limiting how much is read and sent at once ----
    // A watcher or a scan can report every file of a folder at once (66,000 files when a large
    // folder became readable). If each report read its file and sent it immediately, all of them
    // would be in memory together. At most maxConcurrentSends files are read and sent at a time;
    // the others wait holding only their path, and the content is read when their turn comes,
    // so the newest content is sent.
    private sendSlots = Semaphore(DEFAULT_MAX_CONCURRENT_SENDS);
    private sendSlotsLimit = DEFAULT_MAX_CONCURRENT_SENDS;
    private sendsWaiting = 0;
    private sendsActive = 0;
    private sendsDone = 0;
    private sendsDoneReported = 0;
    private sendProgressTimer?: ReturnType<typeof setInterval>;

    private ensureSendSlots() {
        const limit = Math.max(1, Math.floor(this.config.maxConcurrentSends ?? DEFAULT_MAX_CONCURRENT_SENDS));
        if (limit !== this.sendSlotsLimit) {
            this.sendSlots = Semaphore(limit);
            this.sendSlotsLimit = limit;
        }
    }

    sendQueueStatus() {
        return { waiting: this.sendsWaiting, done: this.sendsDone };
    }

    private reportSendProgress() {
        const sent = this.sendsDone - this.sendsDoneReported;
        this.sendsDoneReported = this.sendsDone;
        if (this.sendsWaiting > 0 || sent > 0) {
            this.normalLog(
                `Sending: ${sent} file(s) handled in the last minute, ${this.sendsWaiting} waiting.`,
                LOG_LEVEL_NOTICE,
            );
        }
    }

    private stopSendProgress() {
        if (this.sendProgressTimer !== undefined) clearInterval(this.sendProgressTimer);
        this.sendProgressTimer = undefined;
    }

    private async withSendSlot<T>(proc: () => Promise<T>): Promise<T> {
        this.ensureSendSlots();
        this.sendsWaiting++;
        if (this.sendProgressTimer === undefined) {
            this.sendProgressTimer = setInterval(() => this.reportSendProgress(), SEND_PROGRESS_INTERVAL_MS);
        }
        let release: (() => void) | undefined;
        try {
            release = await this.sendSlots.acquire();
        } finally {
            this.sendsWaiting--;
        }
        this.sendsActive++;
        try {
            return await proc();
        } finally {
            this.sendsActive--;
            this.sendsDone++;
            release();
            // The progress line is only for bulk work; stop it once everything is done.
            if (this.sendsWaiting === 0 && this.sendsActive === 0) this.stopSendProgress();
        }
    }


    constructor(conf: PeerStorageConf, dispatcher: DispatchFun) {
        super(conf, dispatcher);
        // A string would be searched for substrings, and a name with "/" could never match a
        // path segment: refuse both instead of excluding too much or nothing at all.
        const names: unknown = conf.excludeNames;
        if (names !== undefined && (!Array.isArray(names) || names.some((name) => typeof name !== "string"))) {
            throw new Error(`excludeNames of peer "${conf.name}" must be an array of file or folder names`);
        }
        if (conf.excludeNames?.some((name) => name.includes("/"))) {
            throw new Error(`excludeNames of peer "${conf.name}" holds single names; they cannot contain "/"`);
        }
        const dot: unknown = conf.excludeDotNames;
        if (dot !== undefined && typeof dot !== "boolean") {
            throw new Error(`excludeDotNames of peer "${conf.name}" must be true or false`);
        }
    }

    private createPathAccess(): DenoStoragePathAccess | undefined {
        if (getPeerPathNormalizationVersion(this.config) === 0) return undefined;
        return new DenoStoragePathAccess(
            this.toStoragePath(this.toLocalPath(".")),
            this.config.handleFilenameCaseSensitive ?? false,
            (path) => this.readDirectory(path),
        );
    }

    private pathPolicy(access?: DenoStoragePathAccess): PathComparisonSettings | undefined {
        if (!access) return undefined;
        return { pathNormalizationVersion: 1, handleFilenameCaseSensitive: access.handleFilenameCaseSensitive };
    }

    private async storageOperation<T>(
        path: string,
        access: DenoStoragePathAccess | undefined,
        operation: () => Promise<T>,
    ): Promise<T> {
        if (!access) return await operation();
        const key = this.pathIdentityKey(path, this.pathPolicy(access));
        return await withLock(this._getKey(`storage-operation:${key}`), async () => {
            try {
                return await operation();
            } catch (error) {
                this.cache.clear();
                throw error;
            }
        });
    }

    private async resolveStoragePath(pathSrc: string, access?: DenoStoragePathAccess, forWrite = false): Promise<string> {
        if (!access) return this.toStoragePath(this.toLocalPath(pathSrc));
        const input = parseFilePath(pathSrc);
        const path = forWrite ? await access.resolveForWrite(input) : await access.resolveExisting(input);
        if (path === null) throw new Deno.errors.NotFound(`Storage file not found: ${pathSrc}`);
        return access.absolute(path);
    }

    private fileStatKey(pathSrc: string, access?: DenoStoragePathAccess): string {
        if (!access) return `file-stat-${this.toLocalPath(pathSrc)}`;
        return `nfc-file-stat:${JSON.stringify([
            access.handleFilenameCaseSensitive,
            this.pathIdentityKey(pathSrc, this.pathPolicy(access)),
        ])}`;
    }

    private ignorePatterns?: string[];

    // The configured patterns, plus the bare directory form of every `dir/**` pattern.
    // `**/.git/**` matches everything inside `.git` but not `.git` itself, and the
    // directory itself is exactly what has to be recognised to stay out of the tree.
    // As a consequence a trailing `/**` also covers the entry with that name.
    private getIgnorePatterns(): string[] {
        if (!this.ignorePatterns) {
            const configured = this.config.ignore ?? [];
            const directories = configured
                .filter((pattern) => pattern.endsWith("/**"))
                .map((pattern) => pattern.slice(0, -"/**".length));
            this.ignorePatterns = [...configured, ...directories];
        }
        return this.ignorePatterns;
    }

    // Whether a peer-relative POSIX path is excluded from synchronisation.
    // A path is also excluded when any of its ancestor directories is: the scans
    // prune an ignored directory, so live events for its descendants have to be
    // dropped as well, even when a pattern such as `private` or `*.tmp` only
    // matches the directory itself.
    isIgnored(path: string): boolean {
        if (!path) return false;
        const patterns = this.getIgnorePatterns();
        if (patterns.length === 0) return false;
        const segments = path.split("/");
        for (let depth = 1; depth <= segments.length; depth++) {
            const candidate = segments.slice(0, depth).join("/");
            if (patterns.some((pattern) => minimatch(candidate, pattern, IGNORE_MATCH_OPTIONS))) {
                return true;
            }
        }
        return false;
    }

    // ---- Names left out of syncing, in both directions ----
    // A peer-relative POSIX path is excluded when any of its segments is one of `excludeNames`
    // (default: the Synology folders) or, while `excludeDotNames` is on (default), starts with
    // a ".". Unlike `ignore`, which only stops this peer from sending, an excluded path is also
    // never written or deleted here on behalf of another peer. Segments are compared exactly;
    // with pathNormalizationVersion 1 they are compared as NFC, and without case sensitivity
    // (handleFilenameCaseSensitive) also case-folded, like the peer's other path comparisons.
    // Read from the configuration at each call, so nothing has to be invalidated.
    isExcludedPath(path: string): boolean {
        if (!path) return false;
        const names = this.config.excludeNames ?? DEFAULT_EXCLUDE_NAMES;
        const dot = this.config.excludeDotNames ?? true;
        if (!dot && names.length === 0) return false;
        const identity = getPeerPathNormalizationVersion(this.config) === 1;
        const key = identity
            ? (name: string) => {
                const normalized = name.normalize("NFC");
                return this.config.handleFilenameCaseSensitive ? normalized : normalized.toLowerCase();
            }
            : (name: string) => name;
        const excluded = names.map(key);
        for (const segment of path.split("/")) {
            if (segment === "" || segment === "." || segment === "..") continue;
            if (dot && segment.startsWith(".")) return true;
            if (excluded.includes(key(segment))) return true;
        }
        return false;
    }

    // The same for an absolute path as the watchers report it.
    private isExcludedLocal(path: string): boolean {
        return this.isExcludedPath(this.toRelativePath(path));
    }

    // Peer-relative POSIX path of an absolute path as the watchers report it.
    toRelativePath(pathSrc: string) {
        return this.toPosixPath(relative(this.toStoragePath(this.toLocalPath(".")), pathSrc));
    }

    // An incoming change under an excluded name is not an error and not a failed delivery: it is
    // answered with `false` ("nothing was applied", as for a skipped or unchanged write) and is
    // never thrown, so the hub reports no failure and the source has nothing to retry.
    // Nothing on disk is read, written or deleted for it.
    private refuseExcluded(operation: string, path: string): boolean {
        if (!this.isExcludedPath(path)) return false;
        this.debugLog(`${operation} skipped: ${path} (excluded name)`);
        return true;
    }

    async delete(pathSrc: string): Promise<boolean> {
        if (this.refuseExcluded("Delete", pathSrc)) return false;
        const access = this.createPathAccess();
        return await this.storageOperation(pathSrc, access, () => this.deleteFile(pathSrc, access));
    }
    private async deleteFile(pathSrc: string, access?: DenoStoragePathAccess): Promise<boolean> {
        if (this.isUnsafeVaultPath(pathSrc)) {
            this.normalLog(`Delete skipped: ${pathSrc} (the path contains the local path separator)`, LOG_LEVEL_NOTICE);
            return false;
        }
        const policy = this.pathPolicy(access);
        let path: string;
        try {
            path = await this.resolveStoragePath(pathSrc, access);
        } catch (error) {
            if (access && error instanceof Deno.errors.NotFound) {
                // Already gone: remember the deletion so its echo is not sent back.
                await this.applyChange(pathSrc, false, () => Promise.resolve(false), { policy });
                return false;
            }
            throw error;
        }
        // Keyed by the vault path, like dispatchDeleted(), so the resulting watcher event is a repeat.
        // A failed removal is thrown to the caller and is not remembered, so it can be retried.
        return await this.applyChange(pathSrc, false, async () => {
            try {
                await Deno.remove(path);
            } catch (ex) {
                if (ex instanceof Deno.errors.NotFound) return false;
                this.receiveLog(` ${path} delete failed`, LOG_LEVEL_NOTICE);
                throw ex;
            }
            this.receiveLog(` ${path} deleted`);
            this.knownFiles.delete(path);
            void this.runScript(path, true);
            return true;
        }, { policy });
    }
    // A file moved at the source: write the new path first, then remove the old one.
    // The old path is kept when both resolve to the same file (a case-only rename on a
    // case-insensitive filesystem, or the same file under another normalisation).
    // Under an excluded name nothing is written, moved or removed: a move onto an excluded name
    // is refused by put() and leaves the old file as it is (it is not removed, as the new one was
    // not stored), and a move from an excluded name stores the new file but leaves the excluded
    // source alone, without even looking at it.
    override async rename(previousPath: string, path: string, data: FileData): Promise<boolean> {
        const previousExcluded = this.refuseExcluded("Rename of the previous path", previousPath);
        if (this.isUnsafeVaultPath(previousPath) || this.isUnsafeVaultPath(path)) return false;
        const saved = await this.put(path, data);
        if (!saved) return false;
        if (previousExcluded) return saved;
        const access = this.createPathAccess();
        try {
            const previous = await Deno.realPath(await this.resolveStoragePath(previousPath, access));
            const current = await Deno.realPath(await this.resolveStoragePath(path, access));
            if (previous === current) return saved;
        } catch (ex) {
            if (!(ex instanceof Deno.errors.NotFound)) throw ex;
        }
        if (previousPath !== path) await this.delete(previousPath);
        return saved;
    }
    // A deletion reported by `source` is skipped while the source still has a file
    // that is this path (or the same file under another spelling).
    override async deleteFrom(path: string, source: Peer): Promise<boolean> {
        if (this.refuseExcluded("Delete", path)) return false;
        const current = await source.currentPaths(path);
        if (current.includes(path)) return false;
        const access = this.createPathAccess();
        for (const candidate of current) {
            try {
                const oldPath = await Deno.realPath(await this.resolveStoragePath(path, access));
                const newPath = await Deno.realPath(await this.resolveStoragePath(candidate, access));
                if (oldPath === newPath) return false;
            } catch (ex) {
                if (!(ex instanceof Deno.errors.NotFound)) throw ex;
            }
        }
        return await this.delete(path);
    }
    async put(pathSrc: string, data: FileData): Promise<boolean> {
        if (this.refuseExcluded("Write", pathSrc)) return false;
        const access = this.createPathAccess();
        return await this.storageOperation(pathSrc, access, () => this.putFile(pathSrc, data, access));
    }
    private async putFile(pathSrc: string, data: FileData, access?: DenoStoragePathAccess): Promise<boolean> {
        if (this.isUnsafeVaultPath(pathSrc)) {
            this.normalLog(`Write skipped: ${pathSrc} (the path contains the local path separator)`, LOG_LEVEL_NOTICE);
            return false;
        }
        const lp = this.toLocalPath(pathSrc);
        const path = await this.resolveStoragePath(pathSrc, access, true);
        try {
            const incoming = data.data instanceof Uint8Array
                ? data.data
                : new TextEncoder().encode(getDocData(data.data));
            // Preserve existing content when an empty payload conflicts with its reported size.
            // Check before recording repeats so a later valid empty update can still be saved.
            if (incoming.byteLength === 0 && data.size > 0) {
                let existingSize = -1;
                try {
                    existingSize = (await Deno.stat(path)).size;
                } catch (_e) {
                    existingSize = -1;
                }
                if (existingSize > 0) {
                    this.normalLog(
                        `Empty write blocked: ${lp} (${existingSize} bytes on disk, ${data.size} bytes reported, 0 bytes received)`,
                        LOG_LEVEL_NOTICE,
                    );
                    return false;
                }
            }
            // Keyed by the vault path, like dispatch(), so the resulting watcher event is a repeat.
            // The content is remembered only after it was written, so a failed write can be retried.
            return await this.applyChange(pathSrc, data, async () => {
                // Do not rewrite a file whose content is already the same. A catch-up from an
                // older changes-feed position re-delivers every document; the repeat cache holds
                // only a few hundred paths, and a rewrite of identical data would still cost a
                // full copy in the next snapshot of a snapshotting filesystem (Btrfs) and make
                // the watcher or the rescan send every file back. The sizes are compared first
                // so that only a file of the same size is read. This runs inside applyChange, so
                // the check and a write for the same path never interleave, and the content
                // is remembered as a repeat as after a write. It returns true: the content is in
                // place, which rename() needs in order to remove the previous path.
                let existingStat: Deno.FileInfo | undefined;
                try {
                    existingStat = await Deno.stat(path);
                } catch (_e) {
                    existingStat = undefined;
                }
                if (existingStat?.isFile && existingStat.size === incoming.byteLength) {
                    let existing: Uint8Array | undefined;
                    try {
                        existing = await Deno.readFile(path);
                    } catch (_e) {
                        existing = undefined;
                    }
                    if (existing && isSameBytes(existing, incoming)) {
                        this.debugLog(`${lp} unchanged; not rewritten`);
                        // Record what is on disk (its own mtime and size), so that neither the
                        // offline scan nor a rescan finds the file changed.
                        await this.writeFileStat(pathSrc, existingStat, access);
                        return true;
                    }
                }
                // While recursive is true, mkdir will not raise the `AlreadyExist`.
                await Deno.mkdir(dirname(path), { recursive: true });
                const fp = await Deno.open(path, { read: true, write: true, create: true });
                try {
                    let offset = 0;
                    while (offset < incoming.byteLength) offset += await fp.write(incoming.subarray(offset));
                    await fp.truncate(offset);
                    await fp.utime(new Date(data.mtime), new Date(data.mtime));
                } finally {
                    fp.close();
                }
                this.receiveLog(`${lp} saved`);
                await this.writeFileStat(pathSrc, undefined, access);
                void this.runScript(path, false);
                return true;
            }, { policy: this.pathPolicy(access) });
        } catch (ex) {
            Logger(ex, LOG_LEVEL_INFO);
            this.receiveLog(`${lp} save failed`);
            throw ex;
        }
    }

    async runScript(filename: string, isDeleted: boolean): Promise<boolean> {
        if (!this.config.processor) return false;
        if (!this.config.processor.cmd) return false;

        // const result = [];
        try {
            // const startDate = new Date();
            const cmd = this.config.processor.cmd;
            const mode = isDeleted ? "deleted" : "modified";
            const args = this.config.processor.args.map(e => {
                if (e == "$filename") return filename;
                if (e == "$mode") return mode;
                return e
            });
            // const dateStr = startDate.toLocaleString();
            const scriptLineMessage = `Script: called ${cmd} with args ${JSON.stringify(args)}`;
            this.normalLog(`Processor : ${scriptLineMessage}`)
            const command = new Deno.Command(
                cmd, {
                args: args,
                cwd: ".",
                env: {
                    filename: filename,
                    mode: mode
                }
            });
            // const start = performance.now();
            const { code, stdout, stderr } = await command.output();
            // const end = performance.now();
            const stdoutText = new TextDecoder().decode(stdout);
            const stderrText = new TextDecoder().decode(stderr);
            // result.push(`# Processor called: ${dateStr}\n`);
            // result.push(`command: \`${scriptLineMessage}\``);
            if (code === 0) {
                this.normalLog("Processor called: Performed successfully.")
                // result.push("Processor called: Performed successfully.")
                this.normalLog(stdoutText);
            } else {
                this.normalLog("Processor called: Performed but with some errors.")
                // result.push("Processor called: Performed but with some errors.")
                this.normalLog(stderrText, LOG_LEVEL_NOTICE);
            }
            // result.push(`\n- Spent ${Math.ceil(end - start) / 1000} ms`);
            // result.push("## --STDOUT--\n")
            // result.push("```\n" + stdoutText + "\n```");
            // result.push("## --STDERR--n")
            // result.push("```\n" + stderrText + "\n```");
            // const strResult = result.join("\n");
            return true;
        } catch (ex) {
            this.normalLog("Processor: Error on processing");;
            // this.normalLog(ex);
            this.normalLog(JSON.stringify(ex, null, 2));
            return false;
        }

    }

    async get(pathSrc: string, access = this.createPathAccess()): Promise<false | FileData> {
        return await this.storageOperation(pathSrc, access, () => this.getFile(pathSrc, access));
    }
    private async getFile(pathSrc: string, access?: DenoStoragePathAccess): Promise<false | FileData> {
        const path = await this.resolveStoragePath(pathSrc, access);
        const stat = await Deno.stat(path);
        if (!stat.isFile) {
            return false;
        }
        const ret: FileData = {
            ctime: stat.mtime?.getTime() ?? 0,
            mtime: stat.mtime?.getTime() ?? 0,
            size: stat.size,
            data: [],
        };
        if (isPlainText(path)) {
            ret.data = [await Deno.readTextFile(path)];
        } else {
            ret.data = await Deno.readFile(path);
        }
        return ret;
    }
    watcher?: chokidar.FSWatcher;

    private async closeChokidarWatcher(watcher?: chokidar.FSWatcher): Promise<void> {
        if (!watcher) return;
        try {
            await watcher.close();
        } catch (ex) {
            // Chokidar may already have closed the watcher after an error.
            Logger(ex, LOG_LEVEL_VERBOSE);
        }
    }

    // The path as the other peers know it.
    private hubPath(path: string, access?: DenoStoragePathAccess): string {
        return access ? path : this.toGlobalPath(path);
    }

    // ---- Trying again after a failed send ----
    //
    // A change which could not be delivered to every peer (CouchDB restarting or unreachable, a
    // chunk missing there) is not recorded as sent, so the periodic rescan would send it, but that
    // can be an hour away. It is tried again 30, 60, 90, 120 and 150 seconds after the failure
    // (n times sendRetryIntervalSeconds), each time reading the file as it is then. The waiting
    // holds only the path and a timer, never a maxConcurrentSends slot: the slot is taken for the
    // read and the send, when the timer fires. A deletion is retried the same way (it is checked
    // again first, so a file which exists again is not deleted). After the fifth retry fails the
    // path is left unrecorded for the periodic rescan, with a notice. A change which fails for
    // being unreadable is not a failed send and is not retried here.
    private sendRetries = new Map<string, { timer: ReturnType<typeof setTimeout>; deleted: boolean }>();
    private gaveUp = new Set<string>();
    private gaveUpNotices = 0;
    private gaveUpReported = 0;

    sendRetryIntervalMs(): number {
        const seconds = this.config.sendRetryIntervalSeconds ?? DEFAULT_SEND_RETRY_INTERVAL_SECONDS;
        return seconds > 0 ? seconds * 1000 : 0;
    }

    private sendFailed(pathSrc: string, deleted: boolean, retry: number, context: FileChangeContext | undefined, error: unknown) {
        this.clearSendRetry(pathSrc);
        if (this.stopped) return;
        const interval = this.sendRetryIntervalMs();
        if (interval <= 0) return;
        const shown = this.toRelativePath(pathSrc);
        const reason = error instanceof Error ? error.message : `${error}`;
        if (retry >= MAX_SEND_RETRIES) {
            this.gaveUp.add(pathSrc);
            this.gaveUpNotices++;
            if (this.gaveUpNotices <= MAX_GIVE_UP_NOTICES) {
                this.normalLog(
                    `Could not ${deleted ? "send the deletion of" : "send"} ${shown} after ${MAX_SEND_RETRIES} retries (${reason}); ` +
                        `it is left for the periodic rescan.`,
                    LOG_LEVEL_NOTICE,
                );
            } else if (this.gaveUpNotices === MAX_GIVE_UP_NOTICES + 1) {
                this.normalLog(
                    `More files could not be sent after the retries; they are not listed one by one. ` +
                        `The periodic rescan reports how many are left.`,
                    LOG_LEVEL_NOTICE,
                );
            }
            return;
        }
        const attempt = retry + 1;
        const waitMs = attempt * interval;
        this.normalLog(
            `Sending ${deleted ? "the deletion of " : ""}${shown} failed (${reason}); retry ${attempt} of ${MAX_SEND_RETRIES} in ${waitMs / 1000} s.`,
        );
        const generation = this.generation;
        const timer = setTimeout(() => {
            this.sendRetries.delete(pathSrc);
            if (this.stopped || this.generation !== generation) return;
            // Another start of this peer is not this one's business: only the generation which
            // scheduled the retry may send it.
            const isCurrent = () => !this.stopped && this.generation === generation;
            const task = deleted
                ? this.queueDeletion(pathSrc, isCurrent, attempt)
                : this.queueDispatch(pathSrc, context, this.createPathAccess(), isCurrent, attempt);
            // A failure is reported by queueTask, and schedules the next retry itself.
            task.catch(() => {});
        }, waitMs);
        this.sendRetries.set(pathSrc, { timer, deleted });
    }

    // Delivered, so any retry for this path (and the give-up mark) is obsolete.
    private sendSucceeded(pathSrc: string, deleted: boolean) {
        this.clearSendRetry(pathSrc, deleted);
        this.gaveUp.delete(pathSrc);
    }

    // Cancel the waiting retry of a path. With `deleted`, only a retry of that kind: a retry
    // of the other kind is made moot by the path's state, and settles itself when it runs.
    private clearSendRetry(pathSrc: string, deleted?: boolean) {
        const waiting = this.sendRetries.get(pathSrc);
        if (!waiting || (deleted !== undefined && waiting.deleted !== deleted)) return;
        clearTimeout(waiting.timer);
        this.sendRetries.delete(pathSrc);
    }

    private clearSendRetries() {
        for (const { timer } of this.sendRetries.values()) clearTimeout(timer);
        this.sendRetries.clear();
    }

    // How many paths wait for a retry (for tests and status).
    sendRetriesWaiting(): number {
        return this.sendRetries.size;
    }

    dispatch(pathSrc: string, access = this.createPathAccess(), isCurrent = () => true): Promise<void> {
        if (!this.stopped) void this.queueDispatch(pathSrc, undefined, access, isCurrent);
        return Promise.resolve();
    }
    private queueDispatch(
        pathSrc: string,
        context?: FileChangeContext,
        access = this.createPathAccess(),
        isCurrent = () => true,
        retry = 0,
    ): Promise<void> {
        const lP = this.toStoragePath(this.toLocalPath("."));
        const path = this.toPosixPath(relative(lP, pathSrc));
        if (this.isOutsideBaseDir(path)) return Promise.resolve();

        if (this.isIgnored(path)) {
            this.debugLog(`${path} ignored`);
            return Promise.resolve();
        }
        if (this.isExcludedPath(path)) {
            this.debugLog(`${path} excluded`);
            return Promise.resolve();
        }

        return this.queueTask(pathSrc, pathSrc, async () => {
            await delay(250);
            if (!isCurrent()) return;
            // The file is read only when its turn comes, so waiting files hold nothing but their path.
            // It is read inside the origin lock: a change of the same file arriving from another
            // peer is applied first, and what is read here is then the state it left.
            await this.withSendSlot(() => this.withOriginLock(this.originKey(path, access), async () => {
                if (!isCurrent()) return;
                let data: FileData | false;
                try {
                    // Read after waiting so a completed incoming update or deletion
                    // supersedes the earlier watcher event's content.
                    data = await this.get(path, access);
                    if (!isCurrent()) return;
                    if (data !== false && !this.config.useChokidar) {
                        this.knownFiles.add(await this.resolveStoragePath(path, access));
                    }
                } catch (ex) {
                    // Not a failed delivery, so no retry is waiting for it: the file is read again
                    // by the next event or rescan.
                    this.clearSendRetry(pathSrc, false);
                    if (ex instanceof Deno.errors.PermissionDenied && context?.previousPath === undefined) {
                        // Nothing is sent; it is read again by the next rescan.
                        this.noteUnreadable(pathSrc, false, ex);
                        return;
                    }
                    if (!(ex instanceof Deno.errors.NotFound)) {
                        this.normalLog(`File read failed: ${path}`, LOG_LEVEL_NOTICE);
                        Logger(ex, LOG_LEVEL_VERBOSE);
                    }
                    if (context?.previousPath !== undefined) throw ex;
                    return;
                }
                this.clearUnreadable(pathSrc, false);
                if (data === false) {
                    this.clearSendRetry(pathSrc, false);
                    if (context?.previousPath !== undefined) {
                        throw new Error(`Rename target is no longer a file: ${path}`);
                    }
                    return;
                }
                const hubContext = context?.previousPath === undefined
                    ? undefined
                    : { previousPath: this.hubPath(context.previousPath, access) };
                // The content is remembered as handled, and its file record saved, only after every
                // peer accepted it. A failed delivery therefore stays eligible for the next
                // event or offline scan.
                try {
                    await this.applyChange(path, data, async () => {
                        if (!isCurrent()) return false;
                        this.sendLog(`${path} change detected`);
                        await this.dispatchToHub(this, this.hubPath(path, access), data, hubContext);
                        this.recordSent(path, data, access, isCurrent);
                        return true;
                    }, { force: hubContext !== undefined, suppressPendingEcho: true, policy: this.pathPolicy(access) });
                } catch (ex) {
                    // Not recorded as sent (applyChange records only after success), so the next
                    // event or rescan sends it too; in addition it is tried again after a wait.
                    this.sendFailed(pathSrc, false, retry, context, ex);
                    throw ex;
                }
                this.sendSucceeded(pathSrc, false);
            }));
        });
    }
    // The hub's name for the path, for withOriginLock(). hubPath() can refuse a path; the
    // delivery then fails at its own call, as before.
    private originKey(path: string, access?: DenoStoragePathAccess): string {
        try {
            return this.hubPath(path, access);
        } catch {
            return path;
        }
    }
    async dispatchDeleted(pathSrc: string, isCurrent = () => true) {
        if (this.stopped) return;
        await this.queueDeletion(pathSrc, isCurrent);
    }
    private queueDeletion(pathSrc: string, isCurrent = () => true, retry = 0): Promise<void> {
        const lP = this.toStoragePath(this.toLocalPath("."));
        const path = this.toPosixPath(relative(lP, pathSrc));
        if (this.isOutsideBaseDir(path)) return Promise.resolve();
        if (this.isIgnored(path)) {
            this.debugLog(`${path} ignored`);
            return Promise.resolve();
        }
        if (this.isExcludedPath(path)) {
            this.debugLog(`${path} excluded`);
            return Promise.resolve();
        }
        return this.queueTask(pathSrc, pathSrc, async () => {
            await delay(250);
            if (!isCurrent()) return;
            // A file which cannot be read is not a file which is gone.
            if (this.isUnderUnreadable(pathSrc)) {
                this.debugLog(`${path} deletion skipped: it is under a path which cannot be read`);
                return;
            }
            const access = this.createPathAccess();
            // A real or emulated deletion can become stale while it waits behind a write.
            if (!await this.isMissing(pathSrc, access)) {
                this.clearSendRetry(pathSrc, true);
                return;
            }
            if (!isCurrent()) return;
            if (access) {
                try {
                    const existing = await access.resolveExisting(parseFilePath(path));
                    if (!isCurrent()) return;
                    if (existing !== null) {
                        const actualPath = access.absolute(existing);
                        this.knownFiles.delete(resolve(pathSrc));
                        this.knownFiles.add(actualPath);
                        if (actualPath !== resolve(pathSrc)) await this.dispatch(actualPath, access, isCurrent);
                        return;
                    }
                } catch (error) {
                    this.normalLog(`Deletion identity could not be verified: ${path}`, LOG_LEVEL_NOTICE);
                    Logger(error, LOG_LEVEL_VERBOSE);
                    return;
                }
            }
            let recreated = false;
            try {
                await this.withSendSlot(() => this.withOriginLock(this.originKey(path, access), async () => {
                    // A write from another peer may have put the file back while this waited.
                    if (!await this.isMissing(pathSrc, access)) {
                        recreated = true;
                        return false;
                    }
                    return await this.applyChange(path, false, async () => {
                        if (!isCurrent()) return false;
                        this.sendLog(`${path} delete detected`);
                        await this.dispatchToHub(this, this.hubPath(path, access), false);
                        return true;
                    }, { suppressPendingEcho: true, policy: this.pathPolicy(access) });
                }));
            } catch (ex) {
                // The file stays known, so the next event on its folder reconciles it again.
                this.sendFailed(pathSrc, true, retry, undefined, ex);
                throw ex;
            }
            if (recreated) {
                this.clearSendRetry(pathSrc, true);
                return;
            }
            this.sendSucceeded(pathSrc, true);
            this.knownFiles.delete(resolve(pathSrc));
        });
    }

    // A change outside this peer's baseDir must never reach the hub: "../Other/note.md" would be
    // joined onto the other peers' baseDir and change a note outside their folder. Deno 2.6.9's
    // watchFs delivers remove events of one watcher to every other watcher in the process.
    isOutsideBaseDir(path: string) {
        // Backslashes are filename characters on POSIX and separators on Windows.
        const hasParentPrefix = path === ".." || path.startsWith("../") ||
            (SEPARATOR === "\\" && path.startsWith("..\\"));
        if (hasParentPrefix || isAbsolute(path)) {
            this.debugLog(`Ignored a change outside the base directory: ${path}`);
            return true;
        }
        return false;
    }
    // Vault paths always use "/". On Windows, posixFormat(parse()) only converts the separator
    // before the file name ("a\b\c.md" -> "a\b/c.md"), so convert every separator instead.
    toPosixPath(path: string) {
        const ret = this.pathSeparator === "/" ? posixFormat(parse(path)) : path.replaceAll(this.pathSeparator, "/");
        // this.debugLog(`**TOPOSIX ${path} -> ${ret}`)
        return ret;
    }
    // On Windows, a vault path containing "\" (a file name with a backslash, or "a\b/c.md"
    // as written by earlier versions) resolves to a different file: never write or delete it.
    isUnsafeVaultPath(path: string) {
        return this.pathSeparator !== "/" && path.includes(this.pathSeparator);
    }
    toStoragePath(path: string) {
        const ret = resolve(format(posixParse(path)));
        // this.debugLog(`**TOSTORAGE ${path} -> ${ret}`)
        return ret;
    }

    async writeFileStat(pathSrc: string, statSrc?: Deno.FileInfo, access = this.createPathAccess(), isCurrent = () => true) {
        const key = this.fileStatKey(pathSrc, access);
        const path = await this.resolveStoragePath(pathSrc, access);
        const stat = statSrc ?? await Deno.stat(path);
        if (!stat.isFile || !isCurrent()) {
            return false;
        }
        if (!this.config.useChokidar) this.knownFiles.add(path);
        this.saveFileRecord(key, `${stat.mtime?.getTime() ?? 0}-${stat.size}`, pathSrc);
    }

    // Record that the content read for a change reached the other peers. The stamp is taken
    // from the content that was sent, so a later edit is still seen as a change.
    private recordSent(path: string, data: FileData, access?: DenoStoragePathAccess, isCurrent = () => true) {
        if (!isCurrent()) return;
        this.saveFileRecord(this.fileStatKey(path, access), `${data.mtime}-${data.size}`, path);
    }

    private saveFileRecord(key: string, fileStat: string, pathSrc: string) {
        // The record only saves the next offline scan from re-sending this file,
        // so it is queued and saved in batches; PeerState logs a failure.
        try {
            this.setSettingLater(key, fileStat);
        } catch (ex) {
            // Only when the state cannot even accept the record. The file is still sent.
            this.normalLog(`Could not save the file record for ${pathSrc}: ${errorCode(ex)}`, LOG_LEVEL_NOTICE);
        }
    }

    async isChanged(pathSrc: string, access = this.createPathAccess()) {
        const path = await this.resolveStoragePath(pathSrc, access);
        return await this.isChangedExact(pathSrc, path, access);
    }
    private async isChangedExact(pathSrc: string, path: string, access?: DenoStoragePathAccess) {
        const key = this.fileStatKey(pathSrc, access);
        const last = await this.getSetting(key);
        // console.log(`R:${key}`);
        // console.log(`RV:${last}`);

        const stat = await Deno.stat(path);
        if (!stat.isFile) {
            return false;
        }
        if (!last) return true;
        const fileStat = `${stat.mtime?.getTime() ?? 0}-${stat.size}`;
        // console.log(`RVX:${fileStat}`);
        if (last !== fileStat) return true;
        return false;
    }
    watcherDeno?: Deno.FsWatcher;

    private async isMissing(path: string, access = this.createPathAccess()): Promise<boolean> {
        try {
            await Deno.stat(path);
            // With NFC identity the caller resolves the file's actual spelling itself.
            if (access) return false;
            // A case-insensitive filesystem answers for another spelling of the name:
            // the path is missing unless a directory entry has exactly this spelling.
            const relativePath = this.toRelativePath(path);
            return !(await this.currentPaths(relativePath)).includes(relativePath);
        } catch (ex) {
            if (ex instanceof Deno.errors.NotFound) return true;
            // Permission and I/O errors do not establish that the file has gone.
            this.normalLog(`Deletion verification failed: ${path}`, LOG_LEVEL_NOTICE);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return false;
        }
    }

    // Match directory entries to recover their actual spelling on case-insensitive
    // filesystems. Folding discovers candidates; the destination decides document identity.
    // With NFC identity the path resolver decides what the same name is.
    private async currentEntries(
        path: string,
        includeAliases = false,
        access = this.createPathAccess(),
    ): Promise<string[]> {
        if (this.isOutsideBaseDir(path) || this.isUnsafeVaultPath(path)) return [];
        const root = this.toStoragePath(this.toLocalPath("."));
        if (access) {
            if (!path) return [root];
            try {
                const resolved = await access.resolveExisting(parseFilePath(path));
                if (resolved === null) return [];
                const candidate = access.absolute(resolved);
                if (this.isIgnored(this.toRelativePath(candidate))) {
                    throw new Error(`Path inspection blocked by an ignored directory: ${candidate}`);
                }
                return [candidate];
            } catch (ex) {
                if (!(ex instanceof Deno.errors.NotFound) && !(ex instanceof Deno.errors.NotADirectory)) throw ex;
                return [];
            }
        }
        let parents = [root];
        for (const segment of path.split("/").filter(Boolean)) {
            const next: string[] = [];
            for (const parent of parents) {
                try {
                    const matches: string[] = [];
                    for await (const entry of Deno.readDir(parent)) {
                        if (!entry.isSymlink && entry.name.toLowerCase() === segment.toLowerCase()) {
                            matches.push(entry.name);
                        }
                    }
                    const selected = !includeAliases && matches.includes(segment) ? [segment] : matches;
                    for (const name of selected) {
                        const candidate = resolve(parent, name);
                        if (this.isIgnored(this.toRelativePath(candidate))) {
                            throw new Error(`Path inspection blocked by an ignored directory: ${candidate}`);
                        }
                        next.push(candidate);
                    }
                } catch (ex) {
                    if (!(ex instanceof Deno.errors.NotFound) && !(ex instanceof Deno.errors.NotADirectory)) throw ex;
                }
            }
            parents = next;
        }
        return parents;
    }
    // The files that currently answer to `path`: the path itself, plus any other spelling
    // that names the same entry on a case-insensitive filesystem.
    override async currentPaths(path: string): Promise<string[]> {
        const files: string[] = [];
        for (const candidate of await this.currentEntries(path, true)) {
            try {
                if ((await Deno.lstat(candidate)).isFile) {
                    const current = this.toRelativePath(candidate);
                    if (this.isIgnored(current)) {
                        throw new Error(`Deletion verification blocked by an ignored path: ${current}`);
                    }
                    files.push(current);
                }
            } catch (ex) {
                if (!(ex instanceof Deno.errors.NotFound)) throw ex;
            }
        }
        return files;
    }

    // Deno.watchFs can report a folder being created, renamed or removed without reporting
    // the files in it. An event is therefore only a hint: the paths it names are compared
    // with the files on disk and with knownFiles, and the files found (or no longer found)
    // are sent (or deleted). Events arriving within 100 ms are handled together.
    processFile(event: Deno.FsEvent) {
        if (this.stopped) return;
        const watcher = this.watcherDeno;
        const isCurrent = getPeerPathNormalizationVersion(this.config) === 1
            ? () => watcher === undefined || this.watcherDeno === watcher
            : () => true;
        const paths = [...new Set(event.paths.map((path) => resolve(path)))].filter((path) => {
            const relativePath = this.toRelativePath(path);
            if (this.isOutsideBaseDir(relativePath)) return false;
            // Filter before scheduling the task, which stats the path.
            if (this.isIgnored(relativePath)) {
                this.debugLog(`${relativePath} ignored`);
                return false;
            }
            // Deno.watchFs cannot be told to skip a tree, so its events are dropped here, before
            // anything is stat()ed. A move between an excluded and a normal name keeps only the
            // normal side: it is a deletion or a new file as far as the other peers are concerned.
            if (this.isExcludedPath(relativePath)) return false;
            return true;
        });
        if (!paths.length) return;
        const key = `${event.kind}:${paths.join("\0")}`;
        this.pendingEvents.set(key, { ...event, paths });
        void this.queueTask("watcher-events", `${this.operationKey}:scan`, async () => {
            await delay(100);
            const events = [...this.pendingEvents.values()];
            this.pendingEvents.clear();
            if (!isCurrent()) return;
            const access = this.createPathAccess();
            // A paired event establishes a relation. Process it before accompanying
            // remove hints, without inferring a relation between separate events.
            const paired = (event: Deno.FsEvent) => event.kind === "rename" && event.paths.length === 2;
            events.sort((a, b) => Number(paired(b)) - Number(paired(a)));
            const failures: unknown[] = [];
            const blocked: string[] = [];
            const contains = (parent: string, path: string) =>
                path === parent || path.startsWith(`${parent}${SEPARATOR}`);
            for (const accepted of events) {
                if (!isCurrent()) return;
                if (accepted.paths.some((path) => blocked.some((parent) => contains(parent, path) || contains(path, parent)))) {
                    continue;
                }
                try {
                    await this.reconcileEvent(accepted, access, isCurrent);
                } catch (ex) {
                    this.normalLog(`Watcher reconciliation failed: ${accepted.paths.join(", ")}`, LOG_LEVEL_NOTICE);
                    failures.push(ex);
                    // Preserve a failed rename's source while continuing unrelated work.
                    if (paired(accepted)) blocked.push(...accepted.paths);
                }
            }
            if (failures.length) throw new AggregateError(failures, "Watcher reconciliation failed");
        });
    }
    private async reconcileEvent(
        event: Deno.FsEvent,
        access: DenoStoragePathAccess | undefined,
        isCurrent: () => boolean,
    ): Promise<void> {
        const entries = new Map<string, string[]>();
        for (const path of event.paths) {
            try {
                try {
                    await Deno.stat(path);
                } catch (ex) {
                    if (!(ex instanceof Deno.errors.NotFound)) throw ex;
                    // It is gone: what could not be read in it is gone as well.
                    this.forgetUnreadable(path);
                }
                entries.set(path, await this.currentEntries(this.toRelativePath(path), false, access));
            } catch (ex) {
                if (!(ex instanceof Deno.errors.PermissionDenied)) throw ex;
                // The event is only a hint, and what it names cannot be inspected: send nothing,
                // delete nothing, and let the rescans look at it again.
                this.noteUnreadable(path, true, ex);
                return;
            }
        }
        const oldPath = event.kind === "rename" && event.paths.length === 2
            ? event.paths.find((path) => !entries.get(path)!.includes(path))
            : undefined;
        const newPath = oldPath
            ? event.paths.find((path) => path !== oldPath && entries.get(path)!.includes(path))
            : undefined;
        if (oldPath && newPath) {
            await this.reconcilePath(newPath, access, isCurrent, oldPath);
            await this.reconcilePath(oldPath, access, isCurrent, undefined, false);
        } else {
            for (const path of event.paths) {
                await this.reconcilePath(path, access, isCurrent, undefined, event.kind !== "remove");
            }
        }
    }
    // Send every file found at `path` (a file, or a folder whose files are all listed),
    // then delete the known files at or below `path` that are no longer there.
    // `previousPath` is where the folder was before a rename, so its files travel as moves.
    private async reconcilePath(
        path: string,
        access: DenoStoragePathAccess | undefined,
        isCurrent: () => boolean,
        previousPath?: string,
        emitChanges = true,
    ): Promise<void> {
        const currentFiles = new Set<string>();
        let currentEntries: string[] = [];
        if (emitChanges) {
            try {
                currentEntries = await this.currentEntries(this.toRelativePath(path), false, access);
            } catch (ex) {
                if (!(ex instanceof Deno.errors.PermissionDenied)) throw ex;
                // Not being able to look is not evidence that the files are gone: delete nothing.
                this.noteUnreadable(path, true, ex);
                return;
            }
        }
        for (const current of currentEntries) {
            const relativePath = this.toRelativePath(current);
            if (this.isIgnored(relativePath) || this.isExcludedPath(relativePath)) continue;
            let stat: Deno.FileInfo;
            try {
                stat = await Deno.lstat(current);
            } catch (ex) {
                if (ex instanceof Deno.errors.NotFound) continue;
                if (ex instanceof Deno.errors.PermissionDenied) {
                    this.noteUnreadable(current, true, ex);
                    continue;
                }
                throw ex;
            }
            const files = stat.isFile ? [current] : stat.isDirectory ? this.listFiles(current, relativePath, access) : [];
            const saves: Promise<void>[] = [];
            for await (const file of files) {
                currentFiles.add(file);
                this.knownFiles.add(file);
                const oldFile = previousPath ? resolve(previousPath, relative(current, file)) : undefined;
                const context = oldFile && this.knownFiles.has(oldFile)
                    ? { previousPath: this.toRelativePath(oldFile) }
                    : undefined;
                saves.push(this.queueDispatch(file, context, access, isCurrent));
            }
            await Promise.all(saves);
        }
        const prefix = path.endsWith(SEPARATOR) ? path : `${path}${SEPARATOR}`;
        // A folder which could not be listed has no files to compare: its known files stay.
        const removed = [...this.knownFiles].filter((file) =>
            (file === path || file.startsWith(prefix)) && !currentFiles.has(file) && !this.isUnderUnreadable(file)
        );
        await Promise.all(removed.map((file) => this.queueDeletion(file, isCurrent)));
    }

    // Prune ignored and excluded trees before inventorying or inspecting their contents, even
    // when offline uploads are disabled and only the deletion inventory is needed.
    private async *listFiles(directory: string, prefix = "", access = this.createPathAccess()): AsyncGenerator<string> {
        if (access) {
            const unreadable = {
                onUnreadable: (path: StoragePath, error: unknown) => this.noteUnreadable(access.absolute(path), true, error),
                onReadable: (path: StoragePath) => this.clearUnreadable(access.absolute(path), true),
            };
            const skipped = (path: string) => this.isIgnored(path) || this.isExcludedPath(path);
            for await (const path of access.files(skipped, prefix as StoragePath, unreadable)) {
                yield access.absolute(path);
            }
            return;
        }
        try {
            for await (const entry of this.readDirectory(directory)) {
                const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
                if (this.isIgnored(relativePath)) {
                    this.debugLog(`${relativePath} ignored`);
                    continue;
                }
                // Not descended into: an excluded folder (@eaDir) can hold thousands of files.
                if (this.isExcludedPath(relativePath)) {
                    this.debugLog(`${relativePath} excluded`);
                    continue;
                }
                const path = resolve(directory, entry.name);
                if (entry.isFile) yield path;
                else if (entry.isDirectory) yield* this.listFiles(path, relativePath, access);
            }
            this.clearUnreadable(directory, true);
        } catch (ex) {
            // A directory can disappear while the inventory is being read. Continue
            // with its siblings; queued watcher events reconcile any known children.
            if (ex instanceof Deno.errors.NotFound) return;
            // A directory which cannot be read (no permission, an I/O error) is left out, with a
            // notice, and the siblings are listed. It is tried again by the next scan.
            this.noteUnreadable(directory, true, ex);
        }
    }

    // ---- Paths the bridge cannot read ----
    //
    // A folder or file which cannot be read is left out of the scans instead of stopping them,
    // and the peer goes on with the rest. Each such path is reported once; every periodic rescan
    // lists the whole folder again, so a path which became readable is found by it, its files are
    // sent (their record of the last send is missing or differs), and the path is forgotten here.
    // An unreadable path is never taken as a deleted one: no deletion is sent for a file at or
    // below it.
    private unreadable = new Map<string, { path: string; isDirectory: boolean }>();
    private unreadableNotices = 0;
    private unreadableReported = 0;
    private foldersRecovered = false;
    static readonly MAX_UNREADABLE_NOTICES = 20;

    private noteUnreadable(path: string, isDirectory: boolean, reason?: unknown) {
        const key = resolve(path);
        if (this.unreadable.has(key)) return;
        this.unreadable.set(key, { path: key, isDirectory });
        this.unreadableNotices++;
        if (this.unreadableNotices <= PeerStorage.MAX_UNREADABLE_NOTICES) {
            this.normalLog(
                `Cannot read ${isDirectory ? "folder" : "file"} ${key} (${failureReason(reason)}); skipped. ` +
                    `Give the bridge's user read access to sync it; it is tried again at each rescan.`,
                LOG_LEVEL_NOTICE,
            );
        } else if (this.unreadableNotices === PeerStorage.MAX_UNREADABLE_NOTICES + 1) {
            this.normalLog(
                `More paths cannot be read; they are not listed one by one. The periodic rescan reports how many.`,
                LOG_LEVEL_NOTICE,
            );
        }
        if (reason !== undefined) Logger(reason, LOG_LEVEL_VERBOSE);
    }

    // The path was read successfully: forget that it could not be.
    private clearUnreadable(path: string, isDirectory: boolean) {
        if (this.unreadable.size === 0) return;
        if (!this.unreadable.delete(resolve(path))) return;
        if (isDirectory) this.foldersRecovered = true;
        this.normalLog(`${resolve(path)} can be read now.`, LOG_LEVEL_NOTICE);
    }

    private isUnderUnreadable(path: string): boolean {
        if (this.unreadable.size === 0) return false;
        const target = resolve(path);
        for (const [key, entry] of this.unreadable) {
            if (target === key) return true;
            if (entry.isDirectory && target.startsWith(key.endsWith(SEPARATOR) ? key : `${key}${SEPARATOR}`)) return true;
        }
        return false;
    }

    // The path is known to be gone: forget it and what was recorded below it.
    private forgetUnreadable(path: string) {
        if (this.unreadable.size === 0) return;
        const root = resolve(path);
        const prefix = root.endsWith(SEPARATOR) ? root : `${root}${SEPARATOR}`;
        for (const key of [...this.unreadable.keys()]) {
            if (key === root || key.startsWith(prefix)) this.unreadable.delete(key);
        }
    }

    // Forget the paths which have gone since (a path which is still unreadable stays).
    private async pruneUnreadable() {
        for (const [key, entry] of [...this.unreadable]) {
            try {
                await Deno.lstat(entry.path);
            } catch (ex) {
                if (ex instanceof Deno.errors.NotFound) this.unreadable.delete(key);
            }
        }
    }

    // The paths which could not be read at the last look.
    unreadablePaths(): string[] {
        return [...this.unreadable.values()].map((entry) => entry.path);
    }

    // A file whose state could not be inspected. Not found: it went meanwhile. Not readable: it
    // is left out and reported. Anything else is not understood here.
    private skipUnreadable(path: string, ex: unknown) {
        if (ex instanceof Deno.errors.NotFound) return;
        if (ex instanceof Deno.errors.PermissionDenied) {
            this.noteUnreadable(path, false, ex);
            return;
        }
        throw ex;
    }

    // ---- Rescans: recover changes the watcher did not report ----
    //
    // A watcher can miss files: Chokidar reads a new folder before it starts watching it, so
    // files moved into the folder in between are never reported (a folder of 100 files moved
    // into a just-created folder was missed entirely), and Deno.watchFs can drop events. Files
    // in a new folder are therefore picked up by rescanning that folder shortly after it
    // appears (Chokidar only; Deno.watchFs folder events are reconciled by processFile()), and
    // anything else by a periodic rescan of the whole vault, as file synchronisers commonly do.
    //
    // A rescan sends only the files whose mtime and size differ from what was last sent
    // (the rule of the start-up offline scan) and skips the files whose send is already
    // queued or running. It never deletes: a missing file is not evidence of a deletion.

    // Whether a send of this peer-relative path is queued or in progress.
    isSendPending(path: string): boolean {
        const key = this.toStoragePath(this.toLocalPath(path));
        return this.pendingTasks.has(key) || this.sendRetries.has(key);
    }

    // Returns the number of files queued for sending.
    async rescan(dir: string, reason: string): Promise<number> {
        // Inside an excluded folder there is nothing to look for.
        if (this.isExcludedLocal(dir)) return 0;
        const generation = this.generation;
        const isCurrent = () => !this.stopped && this.generation === generation;
        const access = this.createPathAccess();
        let queued = 0;
        let alreadyQueued = 0;
        try {
            for await (const path of this.listFiles(dir, this.toRelativePath(dir), access)) {
                if (!isCurrent()) break;
                if (!this.config.useChokidar) this.knownFiles.add(path);
                // Asking again would only queue a second send behind the current one. A watcher
                // event is not skipped: it reports an edit made after the queued send began.
                // A file waiting for a retry is queued as well: asking again now would stack a second send on it.
                if (this.pendingTasks.has(path) || this.sendRetries.has(path)) {
                    alreadyQueued++;
                    continue;
                }
                const ePath = this.toRelativePath(path);
                try {
                    if (await this.isChangedExact(ePath, path, access)) {
                        this.debugLog(`Rescan (${reason}) found: ${ePath}`);
                        await this.dispatch(path, access, isCurrent);
                        queued++;
                    }
                } catch (ex) {
                    // The file may have been removed or replaced during the scan, or cannot be inspected.
                    if (ex instanceof Deno.errors.PermissionDenied) this.noteUnreadable(path, false, ex);
                    this.debugLog(`Rescan (${reason}) skipped ${ePath}: ${ex instanceof Error ? ex.message : ex}`);
                }
            }
        } catch (ex) {
            this.normalLog(`Rescan (${reason}) of ${dir} stopped: ${ex instanceof Error ? ex.message : ex}`, LOG_LEVEL_NOTICE);
        }
        await this.pruneUnreadable();
        // Once per round, how many paths are still left out: the paths themselves were reported when
        // they were found, and are not listed again.
        if (reason === "periodic" && this.unreadable.size > 0 && this.unreadable.size !== this.unreadableReported) {
            this.normalLog(
                `${this.unreadable.size} path(s) cannot be read and are not synced; they are tried again at the next rescan.`,
                LOG_LEVEL_NOTICE,
            );
        }
        if (reason === "periodic") this.unreadableReported = this.unreadable.size;
        if (reason === "periodic" && this.gaveUp.size > 0 && this.gaveUp.size !== this.gaveUpReported) {
            this.normalLog(
                `${this.gaveUp.size} file(s) could not be sent after the retries; this rescan sends them again.`,
                LOG_LEVEL_NOTICE,
            );
        }
        if (reason === "periodic") this.gaveUpReported = this.gaveUp.size;
        if (queued > 0) {
            this.normalLog(`Rescan (${reason}) queued ${queued} file(s) the watcher had not reported.`, LOG_LEVEL_NOTICE);
        }
        if (alreadyQueued > 0) {
            this.debugLog(`Rescan (${reason}) left ${alreadyQueued} file(s) already queued for sending.`);
        }
        return queued;
    }

    // Chokidar reported a folder as added. The first scan after the watcher is ready covers
    // the folders that exist at start, so earlier events are ignored.
    onAddDir(path: string) {
        if (!this.watcherReady || this.stopped) return;
        const relativePath = this.toRelativePath(path);
        if (!relativePath || this.isOutsideBaseDir(relativePath) || this.isIgnored(relativePath)) return;
        if (this.isExcludedPath(relativePath)) return;
        const previous = this.newDirTimers.get(path);
        if (previous !== undefined) clearTimeout(previous);
        this.newDirTimers.set(
            path,
            setTimeout(() => {
                this.newDirTimers.delete(path);
                if (this.stopped) return;
                void this.rescan(path, "new folder");
            }, NEW_DIR_RESCAN_DELAY_MS),
        );
    }

    // 0 disables the periodic rescan.
    rescanIntervalMs(): number {
        const minutes = this.config.rescanIntervalMinutes ?? DEFAULT_RESCAN_INTERVAL_MINUTES;
        if (!(minutes > 0)) return 0;
        // Randomise between 3/4 and 5/4 of the interval so that peers do not scan at the same moment.
        return minutes * 60_000 * (0.75 + Math.random() * 0.5);
    }

    // Without a watcher there is nothing to complement: drop the timers of the rescans.
    private clearRescanTimers() {
        this.watcherReady = false;
        if (this.rescanTimer !== undefined) clearTimeout(this.rescanTimer);
        this.rescanTimer = undefined;
        for (const timer of this.newDirTimers.values()) clearTimeout(timer);
        this.newDirTimers.clear();
    }

    private isScanReady(): boolean {
        return !!(
            (this.watcher && this.watcherReady) ||
            (this.watcherDeno && this.readyDenoWatcher === this.watcherDeno) ||
            (this.watchDegraded && this.degradedScanDone)
        );
    }

    scheduleFullRescan() {
        if (this.rescanTimer !== undefined) clearTimeout(this.rescanTimer);
        this.rescanTimer = undefined;
        const ms = this.rescanIntervalMs();
        if (ms <= 0) return;
        this.rescanTimer = setTimeout(async () => {
            this.rescanTimer = undefined;
            if (this.stopped) return;
            // Overlapping scans are skipped: so is a round that falls into the start-up scan.
            if (!this.fullScanRunning && this.isScanReady()) {
                this.fullScanRunning = true;
                const generation = this.generation;
                try {
                    // Without a watcher, each round starts by trying to start one again.
                    if (this.watchDegraded) this.replaceDenoWatcher(generation);
                    await this.rescan(this.toStoragePath(this.toLocalPath(".")), "periodic");
                    // A folder which could not be read before may not be watched by the running watcher.
                    if (this.foldersRecovered) {
                        this.foldersRecovered = false;
                        if (this.watcherDeno && !this.watchDegraded) this.replaceDenoWatcher(generation);
                    }
                } finally {
                    this.fullScanRunning = false;
                }
            }
            if (!this.stopped && (this.watcher || this.watcherDeno || this.watchDegraded)) this.scheduleFullRescan();
        }, ms);
    }

    // ---- Deno.watchFs ----
    //
    // The OS may refuse to watch a tree (on Linux, a folder the bridge's user cannot read; also the
    // inotify watch limit), and it is not known whether Deno.watchFs reports that when it is called
    // or later through the events. Both are handled: the peer reports it and goes on with its
    // periodic rescans, which try to start the watcher again. While there is no watcher, files which
    // were added or changed are found by the rescans, but a deleted file is not noticed.

    // Create the watcher, or report why not. The notice is given once per distinct reason.
    private openDenoWatcher(lP: string): Deno.FsWatcher | undefined {
        try {
            const watcher = Deno.watchFs(lP, { recursive: true });
            this.lastWatchProblem = "";
            return watcher;
        } catch (ex) {
            this.reportWatchProblem(`The folder watcher could not be started (${failureReason(ex)})`, ex);
            return undefined;
        }
    }

    private reportWatchProblem(what: string, ex: unknown) {
        if (ex !== undefined) Logger(ex, LOG_LEVEL_VERBOSE);
        const problem = `${what}${ex instanceof Error ? `: ${ex.message}` : ""}`;
        if (problem === this.lastWatchProblem) return;
        this.lastWatchProblem = problem;
        const rescan = this.rescanIntervalMs() > 0
            ? "Files are picked up by the periodic rescan only, and files deleted meanwhile are not noticed. The watcher is tried again at each rescan."
            : "rescanIntervalMinutes is 0, so nothing is picked up until the peer is restarted.";
        this.normalLog(`${problem}. ${rescan}`, LOG_LEVEL_NOTICE);
    }

    // The inventory of the files at start (and when a watcher is started again): remember the existing
    // files, and, with sendChanges, send those which differ from the record of their last send.
    // Returns false when the work became stale (the peer stopped or the watcher was replaced).
    private async inventory(
        lP: string,
        isStale: () => boolean,
        isCurrent: () => boolean,
        sendChanges: boolean,
    ): Promise<boolean> {
        // Even with offline syncing disabled, remember existing files without sending
        // their contents. Unchanged files must also be known for later parent removals.
        const access = this.createPathAccess();
        for await (const path of this.listFiles(lP, "", access)) {
            if (isStale()) return false;
            this.knownFiles.add(path);
            if (sendChanges) {
                const ePath = this.toPosixPath(relative(lP, path));
                try {
                    if (await this.isChangedExact(ePath, path, access)) {
                        this.debugLog(`Offline changes detected: ${ePath}`);
                        await this.dispatch(path, access, isCurrent);
                    }
                } catch (ex) {
                    this.skipUnreadable(path, ex);
                }
            }
        }
        if (isStale()) return false;
        // Records of the files sent by this scan are saved before the scan is called done.
        await this.flushFileRecords();
        return true;
    }

    // Run the watcher: take the inventory, then handle its events until it ends. Whatever ends the
    // events other than stop() leaves the peer on its periodic rescans. A failure of the inventory
    // (a configuration problem such as two names of one path, not an unreadable folder) is thrown
    // to the caller of start() when `foreground` is set, as before; a watcher started again by a
    // rescan has no caller to receive it, so it is reported and the peer stays on its rescans.
    private async runDenoWatcher(
        watcher: Deno.FsWatcher,
        lP: string,
        generation: number,
        sendChanges: boolean,
        foreground: boolean,
    ): Promise<void> {
        this.watcherDeno = watcher;
        this.watchDegraded = false;
        const isStale = () => this.watcherDeno !== watcher;
        const isCurrent = getPeerPathNormalizationVersion(this.config) === 1 ? () => this.watcherDeno === watcher : () => true;
        let failure: unknown;
        let inventoryFailed = false;
        try {
            // Watch before taking the inventory so removals during the scan are queued.
            try {
                if (!await this.inventory(lP, isStale, isCurrent, sendChanges)) return;
            } catch (ex) {
                inventoryFailed = true;
                throw ex;
            }
            this.readyDenoWatcher = watcher;
            for await (const event of watcher) {
                if (this.watcherDeno !== watcher) break;
                this.processFile(event);
            }
        } catch (ex) {
            if (inventoryFailed && foreground) throw ex;
            failure = ex;
        } finally {
            if (this.readyDenoWatcher === watcher) this.readyDenoWatcher = undefined;
            const ended = this.watcherDeno === watcher;
            if (ended) this.watcherDeno = undefined;
            try {
                watcher.close();
            } catch (ex) {
                // The watcher can already be closed by stop() or a concurrent start().
                Logger(ex, LOG_LEVEL_VERBOSE);
            }
            if (ended && inventoryFailed && foreground) {
                this.clearRescanTimers();
            } else if (ended && !this.stopped && this.generation === generation) {
                // The watcher ended without stop() (the OS reported an error, or the events stopped):
                // keep the peer alive on its rescans.
                this.watchDegraded = true;
                this.degradedScanDone = true;
                this.reportWatchProblem(
                    failure === undefined ? "The folder watcher ended" : `The folder watcher failed (${failureReason(failure)})`,
                    failure,
                );
                if (this.rescanTimer === undefined && this.rescanWanted) this.scheduleFullRescan();
            }
        }
    }

    // Try to start the watcher again while the peer is running without one (or to replace the
    // running one, e.g. after a folder became readable). The new watcher takes over at once; the old
    // one is closed. Returns whether a new watcher was started.
    private replaceDenoWatcher(generation: number): boolean {
        if (this.stopped || this.generation !== generation || this.config.useChokidar) return false;
        const lP = this.toStoragePath(this.toLocalPath("."));
        const watcher = this.openDenoWatcher(lP);
        if (!watcher) return false;
        const previous = this.watcherDeno;
        const wasDegraded = this.watchDegraded;
        // The new watcher is registered synchronously (runDenoWatcher sets watcherDeno before it waits).
        const running: Promise<void> = this.runDenoWatcher(watcher, lP, generation, false, false).finally(() => {
            if (this.backgroundWatch === running) this.backgroundWatch = undefined;
        });
        this.backgroundWatch = running;
        if (previous) {
            try {
                previous.close();
            } catch (ex) {
                Logger(ex, LOG_LEVEL_VERBOSE);
            }
        }
        if (wasDegraded) this.normalLog("The folder watcher is running again.", LOG_LEVEL_NOTICE);
        return true;
    }

    private rescanWanted = false;

    async startDenoFsWatch(scheduleRescan = false): Promise<void> {
        const generation = ++this.generation;
        if (this.watcherDeno || this.watcher || this.pendingTasks.size || this.closing) await this.closeAndDrain();
        if (this.generation !== generation) return;
        this.stopped = false;
        this.watchDegraded = false;
        this.rescanWanted = scheduleRescan;
        if (scheduleRescan) this.scheduleFullRescan();
        const lP = this.toStoragePath(this.toLocalPath("."));
        const watcher = this.openDenoWatcher(lP);
        this.normalLog(`Scan offline changes: ${this.config.scanOfflineChanges ? "Enabled, now starting..." : "Disabled"}`);
        if (watcher) {
            await this.runDenoWatcher(watcher, lP, generation, this.config.scanOfflineChanges === true, true);
            return;
        }
        // No watcher: take the inventory anyway, and live on the rescans. The mode is set first, so
        // that a rescan timer which fires during the inventory still reschedules itself.
        this.watchDegraded = true;
        this.degradedScanDone = false;
        const isStale = () => this.stopped || this.generation !== generation;
        if (await this.inventory(lP, isStale, () => !isStale(), this.config.scanOfflineChanges === true)) {
            if (!isStale()) this.degradedScanDone = true;
        }
    }
    async start() {
        // For addressing Deno's and chokidar's compatibility issues (especially on Windows), we use Deno's fs watcher as the primary watcher.
        if (!this.config.useChokidar) {
            await this.startDenoFsWatch(true);
            return;
        }

        const generation = ++this.generation;
        if (this.watcher || this.watcherDeno || this.pendingTasks.size || this.closing) await this.closeAndDrain();
        if (this.generation !== generation) return;
        this.stopped = false;
        this.watcherReady = false;
        this.scheduleFullRescan();
        const lP = this.toStoragePath(this.toLocalPath("."));
        this.normalLog(`Scan offline changes: ${this.config.scanOfflineChanges ? "Enabled, now starting..." : "Disabled"}`);
        const access = this.createPathAccess();
        const watcher = chokidar.watch(lP,
            {
                ignoreInitial: access ? true : !this.config.scanOfflineChanges,
                // Prunes ignored and excluded directories from the watch itself, so their
                // contents are never stat()ed or watched. dispatch() filters as well, for the
                // events which chokidar reports before this can apply.
                ignored: (path: string) => {
                    const relativePath = this.toRelativePath(path);
                    return this.isIgnored(relativePath) || this.isExcludedPath(relativePath);
                },
                awaitWriteFinish: {
                    stabilityThreshold: 500,
                },
            });
        this.watcher = watcher;
        const isCurrent = access ? () => this.watcher === watcher : () => true;

        watcher.on("ready", () => {
            if (this.watcher === watcher) this.watcherReady = true;
        });
        watcher.on("addDir", (path: string) => {
            if (this.watcher === watcher) this.onAddDir(path);
        });
        watcher.on("error", (ex) => {
            if (this.watcher !== watcher) return;
            this.watcher = undefined;
            this.clearRescanTimers();
            if (this.readyChokidarWatcher === watcher) this.readyChokidarWatcher = undefined;
            Logger(ex, LOG_LEVEL_NOTICE);
            void this.closeChokidarWatcher(watcher);
        });
        const changed = async (path: string) => {
            if (!isCurrent()) return;
            const ePath = this.toRelativePath(path);
            if (this.isIgnored(ePath) || this.isExcludedPath(ePath)) return;
            try {
                if (await this.isChanged(ePath, access) && isCurrent()) await this.dispatch(path, access, isCurrent);
            } catch (error) {
                if (!access) throw error;
                this.normalLog(`File inspection failed: ${ePath}`, LOG_LEVEL_NOTICE);
                Logger(error, LOG_LEVEL_VERBOSE);
            }
        };
        watcher.on("change", changed);
        watcher.on("add", changed);
        watcher.on("unlink", async (path) => {
            if (!isCurrent()) return;
            const ePath = this.toPosixPath(relative(this.toLocalPath("."), path));
            if (this.isIgnored(ePath) || this.isExcludedPath(ePath)) {
                this.debugLog(`${ePath} ignored or excluded`);
                return;
            }
            // Confirm that the path is absent before propagating a deletion.
            // Permission and I/O errors do not establish that the file is gone.
            try {
                await Deno.stat(path);
                this.debugLog(`Unlink ignored, file still exists: ${ePath}`);
                return;
            } catch (ex) {
                if (!(ex instanceof Deno.errors.NotFound)) {
                    this.normalLog(`Unlink verification failed: ${ePath}`, LOG_LEVEL_NOTICE);
                    Logger(ex, LOG_LEVEL_VERBOSE);
                    return;
                }
            }
            this.debugLog(`Unlink detected: ${ePath}`);
            if (isCurrent()) await this.dispatchDeleted(path, isCurrent)
        })
        if (access) {
            try {
                for await (const path of this.listFiles(lP, "", access)) {
                    if (this.watcher !== watcher) return;
                    if (this.config.scanOfflineChanges) {
                        const relativePath = this.toRelativePath(path);
                        if (await this.isChangedExact(relativePath, path, access)) await this.dispatch(path, access, isCurrent);
                    }
                }
                await this.flushFileRecords();
                if (this.watcher === watcher) this.readyChokidarWatcher = watcher;
            } catch (error) {
                if (this.watcher === watcher) this.watcher = undefined;
                if (this.readyChokidarWatcher === watcher) this.readyChokidarWatcher = undefined;
                await this.closeChokidarWatcher(watcher);
                throw error;
            }
        }
    }
    // Save the queued file records. Never throws: a failure is logged by PeerState.
    private async flushFileRecords() {
        try {
            await getPeerState().flush();
        } catch (ex) {
            Logger(ex, LOG_LEVEL_VERBOSE);
        }
    }
    async stop() {
        this.generation++;
        await this.closeAndDrain();
    }
    private closeAndDrain(): Promise<void> {
        if (!this.closing) {
            this.closing = this.closeWatchers().finally(() => {
                this.closing = undefined;
            });
        }
        return this.closing;
    }
    // Stop accepting events, close the watchers, and wait for the work already accepted.
    private async closeWatchers(): Promise<void> {
        this.stopped = true;
        this.watchDegraded = false;
        this.degradedScanDone = false;
        this.clearRescanTimers();
        this.clearSendRetries();
        const watcher = this.watcher;
        this.watcher = undefined;
        const watcherDeno = this.watcherDeno;
        this.watcherDeno = undefined;
        this.readyDenoWatcher = undefined;
        this.readyChokidarWatcher = undefined;
        if (watcherDeno) {
            try {
                watcherDeno.close();
            } catch (ex) {
                // The Deno iterator cleanup can close the watcher a second time.
                Logger(ex, LOG_LEVEL_VERBOSE);
            }
        }
        await this.closeChokidarWatcher(watcher);
        await this.backgroundWatch;
        await this.drain();
        this.stopSendProgress();
        await this.flushFileRecords();
    }
    override health(): PeerHealth {
        const chokidarReady = this.watcher && (
            getPeerPathNormalizationVersion(this.config) === 0 || this.readyChokidarWatcher === this.watcher
        );
        const ok = !!(chokidarReady || (this.watcherDeno && this.readyDenoWatcher === this.watcherDeno));
        // No remote backend (backendUp always true). A storage peer still doing its
        // initial offline scan is "starting", not yet healthy, so the base restart
        // logic won't flag it — only a watcher that dies after being healthy counts.
        const detail = ok ? "watching" : this.watchDegraded ? "rescan only (no watcher)" : "starting";
        return { name: this.config.name, type: "storage", ok, detail, backendUp: true, restartWorthy: false };
    }
    // Without a watcher the peer is not healthy in the sense of "watching", but it is working on its
    // rescans, and a restart would not help while the OS keeps refusing the watch (it would only
    // repeat the inventory of the whole folder), so it is never restart-worthy in that state.
    override async probeHealth(): Promise<PeerHealth> {
        if (this.watchDegraded) return this.health();
        return await super.probeHealth();
    }
}
