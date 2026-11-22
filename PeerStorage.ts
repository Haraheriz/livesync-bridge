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
const SEND_PROGRESS_INTERVAL_MS = 60_000;

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
    }

    private createPathAccess(): DenoStoragePathAccess | undefined {
        if (getPeerPathNormalizationVersion(this.config) === 0) return undefined;
        return new DenoStoragePathAccess(
            this.toStoragePath(this.toLocalPath(".")),
            this.config.handleFilenameCaseSensitive ?? false,
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

    // Peer-relative POSIX path of an absolute path as the watchers report it.
    toRelativePath(pathSrc: string) {
        return this.toPosixPath(relative(this.toStoragePath(this.toLocalPath(".")), pathSrc));
    }

    async delete(pathSrc: string): Promise<boolean> {
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
    override async rename(previousPath: string, path: string, data: FileData): Promise<boolean> {
        if (this.isUnsafeVaultPath(previousPath) || this.isUnsafeVaultPath(path)) return false;
        const saved = await this.put(path, data);
        if (!saved) return false;
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

    dispatch(pathSrc: string, access = this.createPathAccess(), isCurrent = () => true): Promise<void> {
        if (!this.stopped) void this.queueDispatch(pathSrc, undefined, access, isCurrent);
        return Promise.resolve();
    }
    private queueDispatch(
        pathSrc: string,
        context?: FileChangeContext,
        access = this.createPathAccess(),
        isCurrent = () => true,
    ): Promise<void> {
        const lP = this.toStoragePath(this.toLocalPath("."));
        const path = this.toPosixPath(relative(lP, pathSrc));
        if (this.isOutsideBaseDir(path)) return Promise.resolve();

        if (this.isIgnored(path)) {
            this.debugLog(`${path} ignored`);
            return Promise.resolve();
        }

        return this.queueTask(pathSrc, pathSrc, async () => {
            await delay(250);
            if (!isCurrent()) return;
            // The file is read only when its turn comes, so waiting files hold nothing but their path.
            await this.withSendSlot(async () => {
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
                    if (!(ex instanceof Deno.errors.NotFound)) {
                        this.normalLog(`File read failed: ${path}`, LOG_LEVEL_NOTICE);
                        Logger(ex, LOG_LEVEL_VERBOSE);
                    }
                    if (context?.previousPath !== undefined) throw ex;
                    return;
                }
                if (data === false) {
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
                await this.applyChange(path, data, async () => {
                    if (!isCurrent()) return false;
                    this.sendLog(`${path} change detected`);
                    await this.dispatchToHub(this, this.hubPath(path, access), data, hubContext);
                    this.recordSent(path, data, access, isCurrent);
                    return true;
                }, { force: hubContext !== undefined, suppressPendingEcho: true, policy: this.pathPolicy(access) });
            });
        });
    }
    async dispatchDeleted(pathSrc: string, isCurrent = () => true) {
        if (this.stopped) return;
        await this.queueDeletion(pathSrc, isCurrent);
    }
    private queueDeletion(pathSrc: string, isCurrent = () => true): Promise<void> {
        const lP = this.toStoragePath(this.toLocalPath("."));
        const path = this.toPosixPath(relative(lP, pathSrc));
        if (this.isOutsideBaseDir(path)) return Promise.resolve();
        if (this.isIgnored(path)) {
            this.debugLog(`${path} ignored`);
            return Promise.resolve();
        }
        return this.queueTask(pathSrc, pathSrc, async () => {
            await delay(250);
            if (!isCurrent()) return;
            const access = this.createPathAccess();
            // A real or emulated deletion can become stale while it waits behind a write.
            if (!await this.isMissing(pathSrc, access) || !isCurrent()) return;
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
            await this.withSendSlot(() =>
                this.applyChange(path, false, async () => {
                    if (!isCurrent()) return false;
                    this.sendLog(`${path} delete detected`);
                    await this.dispatchToHub(this, this.hubPath(path, access), false);
                    return true;
                }, { suppressPendingEcho: true, policy: this.pathPolicy(access) })
            );
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
                await Deno.stat(path);
            } catch (ex) {
                if (!(ex instanceof Deno.errors.NotFound)) throw ex;
            }
            entries.set(path, await this.currentEntries(this.toRelativePath(path), false, access));
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
        for (const current of emitChanges ? await this.currentEntries(this.toRelativePath(path), false, access) : []) {
            const relativePath = this.toRelativePath(current);
            if (this.isIgnored(relativePath)) continue;
            let stat: Deno.FileInfo;
            try {
                stat = await Deno.lstat(current);
            } catch (ex) {
                if (ex instanceof Deno.errors.NotFound) continue;
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
        const removed = [...this.knownFiles].filter((file) =>
            (file === path || file.startsWith(prefix)) && !currentFiles.has(file)
        );
        await Promise.all(removed.map((file) => this.queueDeletion(file, isCurrent)));
    }

    // Prune ignored trees before inventorying or inspecting their contents, even
    // when offline uploads are disabled and only the deletion inventory is needed.
    private async *listFiles(directory: string, prefix = "", access = this.createPathAccess()): AsyncGenerator<string> {
        if (access) {
            for await (const path of access.files((path) => this.isIgnored(path), prefix as StoragePath)) {
                yield access.absolute(path);
            }
            return;
        }
        try {
            for await (const entry of Deno.readDir(directory)) {
                const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
                if (this.isIgnored(relativePath)) {
                    this.debugLog(`${relativePath} ignored`);
                    continue;
                }
                const path = resolve(directory, entry.name);
                if (entry.isFile) yield path;
                else if (entry.isDirectory) yield* this.listFiles(path, relativePath, access);
            }
        } catch (ex) {
            // A directory can disappear while the inventory is being read. Continue
            // with its siblings; queued watcher events reconcile any known children.
            if (!(ex instanceof Deno.errors.NotFound)) throw ex;
        }
    }

    async startDenoFsWatch(): Promise<void> {
        const generation = ++this.generation;
        if (this.watcherDeno || this.watcher || this.pendingTasks.size || this.closing) await this.closeAndDrain();
        if (this.generation !== generation) return;
        this.stopped = false;
        const lP = this.toStoragePath(this.toLocalPath("."));
        const watcher = Deno.watchFs(lP, { recursive: true });
        this.watcherDeno = watcher;

        try {
            this.normalLog(`Scan offline changes: ${this.config.scanOfflineChanges ? "Enabled, now starting..." : "Disabled"}`);
            // Watch before taking the inventory so removals during the scan are queued.
            // Even with offline syncing disabled, remember existing files without sending
            // their contents. Unchanged files must also be known for later parent removals.
            const access = this.createPathAccess();
            const isCurrent = access ? () => this.watcherDeno === watcher : () => true;
            for await (const path of this.listFiles(lP, "", access)) {
                if (this.watcherDeno !== watcher) return;
                this.knownFiles.add(path);
                if (this.config.scanOfflineChanges) {
                    const ePath = this.toPosixPath(relative(lP, path));
                    try {
                        if (await this.isChangedExact(ePath, path, access)) {
                            this.debugLog(`Offline changes detected: ${ePath}`);
                            await this.dispatch(path, access, isCurrent);
                        }
                    } catch (ex) {
                        if (!(ex instanceof Deno.errors.NotFound)) throw ex;
                    }
                }
            }
            if (this.watcherDeno !== watcher) return;
            // Records of the files sent by this scan are saved before the scan is called done.
            await this.flushFileRecords();
            this.readyDenoWatcher = watcher;
            for await (const event of watcher) {
                if (this.watcherDeno !== watcher) break;
                this.processFile(event);
            }
        } finally {
            if (this.readyDenoWatcher === watcher) this.readyDenoWatcher = undefined;
            if (this.watcherDeno === watcher) {
                this.watcherDeno = undefined;
            }
            try {
                watcher.close();
            } catch (ex) {
                // The watcher can already be closed by stop() or a concurrent start().
                Logger(ex, LOG_LEVEL_VERBOSE);
            }
        }

    }
    async start() {
        // For addressing Deno's and chokidar's compatibility issues (especially on Windows), we use Deno's fs watcher as the primary watcher.
        if (!this.config.useChokidar) {
            await this.startDenoFsWatch();
            return;
        }

        const generation = ++this.generation;
        if (this.watcher || this.watcherDeno || this.pendingTasks.size || this.closing) await this.closeAndDrain();
        if (this.generation !== generation) return;
        this.stopped = false;
        const lP = this.toStoragePath(this.toLocalPath("."));
        this.normalLog(`Scan offline changes: ${this.config.scanOfflineChanges ? "Enabled, now starting..." : "Disabled"}`);
        const access = this.createPathAccess();
        const watcher = chokidar.watch(lP,
            {
                ignoreInitial: access ? true : !this.config.scanOfflineChanges,
                // Prunes ignored directories from the watch itself, so their contents
                // are never stat()ed or watched. dispatch() filters as well, for the
                // events which chokidar reports before this can apply.
                ignored: (path: string) => this.isIgnored(this.toRelativePath(path)),
                awaitWriteFinish: {
                    stabilityThreshold: 500,
                },
            });
        this.watcher = watcher;
        const isCurrent = access ? () => this.watcher === watcher : () => true;

        watcher.on("error", (ex) => {
            if (this.watcher !== watcher) return;
            this.watcher = undefined;
            if (this.readyChokidarWatcher === watcher) this.readyChokidarWatcher = undefined;
            Logger(ex, LOG_LEVEL_NOTICE);
            void this.closeChokidarWatcher(watcher);
        });
        const changed = async (path: string) => {
            if (!isCurrent()) return;
            const ePath = this.toRelativePath(path);
            if (this.isIgnored(ePath)) return;
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
            if (this.isIgnored(ePath)) {
                this.debugLog(`${ePath} ignored`);
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
        return { name: this.config.name, type: "storage", ok, detail: ok ? "watching" : "starting", backendUp: true, restartWorthy: false };
    }
}
