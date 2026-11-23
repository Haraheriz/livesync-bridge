import { dirname, join } from "@std/path";
import { DatabaseSync } from "node:sqlite";
import { defaultLogger, LOG_LEVEL_INFO, LOG_LEVEL_NOTICE, setGlobalLogFunction } from "octagonal-wheels/common/logger";
import { PeerStorage } from "./PeerStorage.ts";
import { closePeerState, getPeerState, initPeerState, isNasFileRecord, nasSettingsCandidates } from "./PeerState.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

// The earlier NAS build kept peer records in lsb-settings.sqlite, a single table
// settings(k TEXT PRIMARY KEY, v TEXT NOT NULL) in WAL mode. These tests build
// such files, import them, and check what the storage peer then reads.

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}

function assert(condition: unknown, message: string) {
    if (!condition) throw new Error(message);
}

const OLD_MARKER = "\u0000lsb:migrated-from-localStorage";
const SINCE_KEY = "couch-couchdb--since";
const CREATED_KEY = "couch-couchdb--remote-created";
const STAT_PREFIX = "storage-storage-./data/-file-stat-";
// The rows of the first entries of a real file written by the earlier build.
const SINCE_VALUE =
    "1351-g1AAAACReJzLYWBgYMpgTmHgzcvPy09JdcjLz8gvLskBCScyJNX___8_K4M5CcjrzQWKsRulmRmkJKaiq8dhQh4LkGRoAFL_EQbtAhuUbGBoZmxpia4tCwA4tyvO";
const SAMPLE_ROWS: [string, string][] = [
    [OLD_MARKER, "1795378520463"],
    [CREATED_KEY, "1795406902173"],
    [SINCE_KEY, SINCE_VALUE],
    [`${STAT_PREFIX}data/はじめに.md`, "1795357533909-113"],
    [`${STAT_PREFIX}data/bulk/フォルダ_1/ノート1.md`, "1795407128397-822"],
    [`${STAT_PREFIX}data/bulk/フォルダ_1/ノート2.md`, "1795407128046-823"],
];

async function withDir(fn: (dir: string) => Promise<void>) {
    const dir = await Deno.makeTempDir({ prefix: "lsb-nas-import-test-" });
    try {
        await fn(dir);
    } finally {
        await closePeerState();
        setGlobalLogFunction(defaultLogger);
        await Deno.remove(dir, { recursive: true }).catch(() => {});
    }
}

function captureLogs() {
    const logs: { message: string; level: number | undefined }[] = [];
    setGlobalLogFunction((message, level) => {
        logs.push({ message: String(message), level });
    });
    return logs;
}

// Write a settings file the way the earlier build did. With walOnly the rows
// stay in the write-ahead log: the main file is copied while the writer is
// still open and nothing was checkpointed, like a file whose writer was killed.
function writeOldSettings(target: string, rows: [string, string][], walOnly = false) {
    const building = walOnly ? `${target}.build-${crypto.randomUUID()}` : target;
    const database = new DatabaseSync(building);
    try {
        database.exec("PRAGMA journal_mode=WAL");
        database.exec("PRAGMA wal_autocheckpoint=0");
        database.exec("CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)");
        const insert = database.prepare("INSERT INTO settings (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v");
        for (const [k, v] of rows) insert.run(k, v);
        if (walOnly) {
            Deno.copyFileSync(building, target);
            Deno.copyFileSync(`${building}-wal`, `${target}-wal`);
        }
    } finally {
        database.close();
    }
    if (walOnly) {
        for (const suffix of ["", "-wal", "-shm"]) {
            try {
                Deno.removeSync(`${building}${suffix}`);
            } catch { /* the writer may have removed its own log */ }
        }
    }
}

function countRows(path: string): number {
    const database = new DatabaseSync(path, { readOnly: true });
    try {
        return Number((database.prepare("SELECT COUNT(*) AS n FROM settings").get() as { n: number }).n);
    } finally {
        database.close();
    }
}

async function snapshot(path: string) {
    const read = (file: string) => Deno.readFile(file).catch(() => new Uint8Array());
    return { main: await read(path), wal: await read(`${path}-wal`) };
}

function sameBytes(a: Uint8Array, b: Uint8Array) {
    return a.byteLength === b.byteLength && a.every((v, i) => v === b[i]);
}

const noPeerSend = () => Promise.resolve();

Deno.test("file records of the earlier NAS build are imported, also when they are only in its WAL", () =>
    withDir(async (dir) => {
        const old = join(dir, "lsb-settings.sqlite");
        writeOldSettings(old, [
            ...SAMPLE_ROWS,
            ["couch-couchdb--pending-loads", "[]"],
            ["something-unknown", "x"],
            [`${STAT_PREFIX}data/broken.md`, "not-a-stamp"],
        ], true);
        const wal = await Deno.stat(`${old}-wal`);
        assert(wal.size > 0, "the fixture must have a write-ahead log");
        const mainText = new TextDecoder().decode(await Deno.readFile(old));
        assert(!mainText.includes("はじめに.md"), "the records must be only in the log, not in the main file");

        const logs = captureLogs();
        const state = await initPeerState({ filePath: join(dir, "state.sqlite"), legacy: undefined });
        assertEquals(await state.getValue(`${STAT_PREFIX}data/はじめに.md`), "1795357533909-113", "a file record is imported unchanged");
        assertEquals(await state.getValue(`${STAT_PREFIX}data/bulk/フォルダ_1/ノート1.md`), "1795407128397-822", "every file record is imported");
        assertEquals(await state.getValue(`${STAT_PREFIX}data/bulk/フォルダ_1/ノート2.md`), "1795407128046-823", "even the last one");
        // Not carried over: see the comment above NAS_FILE_STAT_KEY in PeerState.ts.
        assertEquals(await state.getValue(SINCE_KEY), null, "the unbound changes-feed position is not imported");
        assertEquals(await state.getValue(CREATED_KEY), null, "remote-created is not imported");
        assertEquals(await state.getValue("couch-couchdb--pending-loads"), null, "the old retry queue is not imported");
        assertEquals(await state.getValue(OLD_MARKER), null, "the old build's own marker is not imported");
        assertEquals(await state.getValue("something-unknown"), null, "unknown entries are not imported");
        assertEquals(await state.getValue(`${STAT_PREFIX}data/broken.md`), null, "a record that is not mtime-size is not imported");

        const info = logs.filter((log) => log.message.includes("earlier NAS build"));
        assertEquals(info.length, 1, "one line reports the import");
        assertEquals(info[0].level, LOG_LEVEL_INFO, "it is an information line");
        assert(info[0].message.includes("Imported 3 file records") && info[0].message.includes("6 other entries"), `the counts are reported (${info[0].message})`);
        assert(!info[0].message.includes(dir) && !info[0].message.includes("はじめに"), "the line names no path");
    }));

Deno.test("the import runs once: a later change of the old file is not imported", () =>
    withDir(async (dir) => {
        const old = join(dir, "lsb-settings.sqlite");
        const file = join(dir, "state.sqlite");
        writeOldSettings(old, SAMPLE_ROWS);
        await initPeerState({ filePath: file, legacy: undefined });
        await closePeerState();
        writeOldSettings(old, [[`${STAT_PREFIX}data/added-later.md`, "1-2"], [`${STAT_PREFIX}data/はじめに.md`, "9-9"]]);

        const logs = captureLogs();
        const state = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await state.getValue(`${STAT_PREFIX}data/added-later.md`), null, "the marker keeps a second import away");
        assertEquals(await state.getValue(`${STAT_PREFIX}data/はじめに.md`), "1795357533909-113", "and the first import stays as it was");
        assertEquals(logs.filter((log) => log.message.includes("earlier NAS build")).length, 0, "nothing is reported the second time");
    }));

Deno.test("records already in the state win; missing ones are still imported", () =>
    withDir(async (dir) => {
        const file = join(dir, "state.sqlite");
        const first = await initPeerState({ filePath: file, legacy: undefined });
        await first.setValue(`${STAT_PREFIX}data/はじめに.md`, "1795999999999-200");
        await closePeerState();

        writeOldSettings(join(dir, "lsb-settings.sqlite"), SAMPLE_ROWS);
        const state = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await state.getValue(`${STAT_PREFIX}data/はじめに.md`), "1795999999999-200", "the record the new build wrote is kept");
        assertEquals(await state.getValue(`${STAT_PREFIX}data/bulk/フォルダ_1/ノート1.md`), "1795407128397-822", "a record it did not have is imported");

        // The marker is set although some records were not taken over.
        await closePeerState();
        writeOldSettings(join(dir, "lsb-settings.sqlite"), [[`${STAT_PREFIX}data/added-later.md`, "1-2"]]);
        const again = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await again.getValue(`${STAT_PREFIX}data/added-later.md`), null, "no second import");
    }));

Deno.test("the earlier file is left as it was, and is still a readable settings file", () =>
    withDir(async (dir) => {
        const old = join(dir, "lsb-settings.sqlite");
        writeOldSettings(old, SAMPLE_ROWS, true);
        const before = await snapshot(old);
        assert(before.wal.byteLength > 0, "the fixture keeps its rows in the log");
        const rowsBefore = countRows(old);

        await initPeerState({ filePath: join(dir, "state.sqlite"), legacy: undefined });
        await closePeerState();
        await initPeerState({ filePath: join(dir, "state.sqlite"), legacy: undefined, reset: true });

        const after = await snapshot(old);
        assert(sameBytes(before.main, after.main), "the main file is byte-identical");
        assert(sameBytes(before.wal, after.wal), "the write-ahead log is byte-identical");
        assertEquals(countRows(old), rowsBefore, "the row count is the same");
        assertEquals(rowsBefore, SAMPLE_ROWS.length, "and the rows are all still there");
    }));

Deno.test("a missing earlier file is a no-op, and is picked up when it appears", () =>
    withDir(async (dir) => {
        const file = join(dir, "state.sqlite");
        const logs = captureLogs();
        const state = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await state.importNasSettings([join(dir, "lsb-settings.sqlite")]), undefined, "nothing to import");
        assertEquals(logs.length, 0, "nothing is logged when there is no earlier file");
        await closePeerState();

        // No marker was written, so a file that turns up later is still imported once.
        writeOldSettings(join(dir, "lsb-settings.sqlite"), SAMPLE_ROWS);
        const later = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await later.getValue(`${STAT_PREFIX}data/はじめに.md`), "1795357533909-113", "imported when it appears");
    }));

Deno.test("an unreadable earlier file does not stop the start-up and is tried again next time", () =>
    withDir(async (dir) => {
        const old = join(dir, "lsb-settings.sqlite");
        const file = join(dir, "state.sqlite");
        await Deno.writeFile(old, new TextEncoder().encode("this is not a database\n".repeat(400)));
        const garbage = await snapshot(old);

        const logs = captureLogs();
        const state = await initPeerState({ filePath: file, legacy: undefined });
        const notices = logs.filter((log) => log.level === LOG_LEVEL_NOTICE && log.message.includes("earlier NAS build"));
        assertEquals(notices.length, 1, "a notice says the import was not done");
        assert(!notices[0].message.includes(dir), "and names no path");
        await state.setValue("k", "v");
        assertEquals(await state.getValue("k"), "v", "the state works, empty as it was");
        assert(sameBytes(garbage.main, (await snapshot(old)).main), "the unreadable file is not touched");

        // A valid database without the settings table is not a settings file either.
        await closePeerState();
        await Deno.remove(old);
        new DatabaseSync(old).close();
        const logs2 = captureLogs();
        await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(logs2.filter((log) => log.level === LOG_LEVEL_NOTICE).length, 1, "a database without the table gives a notice too");

        // The marker was not set, so a repaired file is imported at the next start.
        await closePeerState();
        await Deno.remove(old);
        writeOldSettings(old, SAMPLE_ROWS);
        const repaired = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await repaired.getValue(`${STAT_PREFIX}data/はじめに.md`), "1795357533909-113", "imported once it can be read");
        assertEquals(await repaired.getValue("k"), "v", "records already saved are kept");
    }));

Deno.test("--reset does not import the earlier file, neither then nor at later starts", () =>
    withDir(async (dir) => {
        const old = join(dir, "lsb-settings.sqlite");
        const file = join(dir, "state.sqlite");
        writeOldSettings(old, SAMPLE_ROWS);
        const logs = captureLogs();
        const state = await initPeerState({ filePath: file, legacy: undefined, reset: true });
        assertEquals(await state.getValue(`${STAT_PREFIX}data/はじめに.md`), null, "a reset starts empty");
        await closePeerState();
        const later = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await later.getValue(`${STAT_PREFIX}data/はじめに.md`), null, "and the file is not imported at the next start");
        assertEquals(logs.filter((log) => log.message.includes("earlier NAS build")).length, 0, "nothing is reported");
        assertEquals(countRows(old), SAMPLE_ROWS.length, "the earlier file stays in place");
    }));

Deno.test("a reset after an import removes the imported records for good", () =>
    withDir(async (dir) => {
        const file = join(dir, "state.sqlite");
        writeOldSettings(join(dir, "lsb-settings.sqlite"), SAMPLE_ROWS);
        await initPeerState({ filePath: file, legacy: undefined });
        await closePeerState();
        const reset = await initPeerState({ filePath: file, legacy: undefined, reset: true });
        assertEquals(await reset.getValue(`${STAT_PREFIX}data/はじめに.md`), null, "the imported record is gone");
        await closePeerState();
        const later = await initPeerState({ filePath: file, legacy: undefined });
        assertEquals(await later.getValue(`${STAT_PREFIX}data/はじめに.md`), null, "and is not imported again");
    }));

Deno.test("LSB_SETTINGS_DB and the state directory are the places searched", async () => {
    const env = (values: Record<string, string>) => (key: string) => values[key];
    assertEquals(
        nasSettingsCandidates(join("x", "state.sqlite"), env({})).join("|"),
        join("x", "lsb-settings.sqlite"),
        "the file next to the state file",
    );
    assertEquals(
        nasSettingsCandidates(join("x", "state.sqlite"), env({ LSB_SETTINGS_DB: "elsewhere/s.sqlite" })).join("|"),
        `elsewhere/s.sqlite|${join("x", "lsb-settings.sqlite")}`,
        "the variable of the earlier build comes first",
    );
    await withDir(async (dir) => {
        const elsewhere = join(dir, "other", "settings.sqlite");
        await Deno.mkdir(dirname(elsewhere));
        writeOldSettings(elsewhere, SAMPLE_ROWS);
        const previous = Deno.env.get("LSB_SETTINGS_DB");
        Deno.env.set("LSB_SETTINGS_DB", elsewhere);
        try {
            const state = await initPeerState({ filePath: join(dir, "state.sqlite"), legacy: undefined });
            assertEquals(await state.getValue(`${STAT_PREFIX}data/はじめに.md`), "1795357533909-113", "the file named by the variable is imported");
        } finally {
            if (previous === undefined) Deno.env.delete("LSB_SETTINGS_DB");
            else Deno.env.set("LSB_SETTINGS_DB", previous);
        }
    });
});

Deno.test("only file records of storage peers have a counterpart in this version", () => {
    assert(isNasFileRecord(`${STAT_PREFIX}data/a.md`, "1795357533909-113"), "a file record");
    assert(isNasFileRecord("my-nas-storage-/app/data/-file-stat-sub dir/a-b.md", "0-0"), "a name and a base directory with dashes");
    assert(!isNasFileRecord(`${STAT_PREFIX}data/a.md`, "1795357533909"), "a value without a size");
    assert(!isNasFileRecord(`${STAT_PREFIX}data/a.md`, "1795357533909-113 "), "a value with extra characters");
    assert(!isNasFileRecord(`${STAT_PREFIX}data/a.md`, new Uint8Array([1])), "a value that is not text");
    assert(!isNasFileRecord(SINCE_KEY, "1-2"), "a CouchDB position");
    assert(!isNasFileRecord(OLD_MARKER, "1795378520463"), "the old marker");
    assert(!isNasFileRecord("storage-storage-./data/-file-stat-", "1-2"), "a record without a path");
});

// The same file the earlier build wrote on the NAS test rig can be given in
// LSB_NAS_SAMPLE_DIR (a directory holding a copy of lsb-settings.sqlite); the
// test then runs on it. Without it, the rows quoted above stand in for it.
async function sampleFile(dir: string): Promise<{ path: string; real: boolean }> {
    const target = join(dir, "lsb-settings.sqlite");
    const source = Deno.env.get("LSB_NAS_SAMPLE_DIR");
    if (source) {
        const original = join(source, "lsb-settings.sqlite");
        for (const suffix of ["", "-wal", "-shm"]) {
            await Deno.copyFile(`${original}${suffix}`, `${target}${suffix}`).catch((ex) => {
                if (suffix === "") throw ex;
            });
        }
        return { path: target, real: true };
    }
    writeOldSettings(target, SAMPLE_ROWS, true);
    return { path: target, real: false };
}

Deno.test("the record of a file kept by the earlier build is readable through the storage peer", () =>
    withDir(async (dir) => {
        const { real } = await sampleFile(dir);
        const logs = captureLogs();
        await initPeerState({ filePath: join(dir, "state.sqlite"), legacy: undefined });
        const conf: PeerStorageConf = { type: "storage", name: "storage", baseDir: "./data/", scanOfflineChanges: true };
        const peer = new PeerStorage(conf, noPeerSend);
        const key = (peer as unknown as { fileStatKey(path: string): string }).fileStatKey("はじめに.md");
        assertEquals(key, "file-stat-data/はじめに.md", "the key PeerStorage builds for this file");
        assertEquals(await peer.getSetting(key), "1795357533909-113", "the record is read through Peer.getSetting");
        const line = logs.find((log) => log.message.includes("earlier NAS build"))?.message ?? "";
        assert(line.includes(real ? "Imported 359 file records" : "Imported 3 file records"), `the import is reported (${line})`);
        assert(line.includes("; 3 other entries"), `the marker, position and creation time are not carried over (${line})`);
        assertEquals(await getPeerState().getValue(SINCE_KEY), null, "the changes-feed position is not imported");
    }));

// The data-loss case: a file the earlier build had recorded must not be sent
// by the start-up scan, while a file changed since is.
Deno.test("an offline scan does not send a file the earlier build had recorded, and sends one that changed", () =>
    withDir(async (dir) => {
        const vault = join(dir, "vault");
        await Deno.mkdir(vault);
        const mtime = new Date(1_700_000_000_000);
        const keep = join(vault, "recorded.md");
        const edited = join(vault, "edited.md");
        const unknown = join(vault, "unknown.md");
        await Deno.writeTextFile(keep, "same as recorded");
        await Deno.writeTextFile(edited, "edited after the record");
        await Deno.writeTextFile(unknown, "never recorded");
        for (const path of [keep, edited, unknown]) await Deno.utime(path, mtime, mtime);

        const conf: PeerStorageConf = { type: "storage", name: "storage", baseDir: vault + "/", scanOfflineChanges: true };
        // The key the earlier build wrote: <name>-<type>-<baseDir>-file-stat-<local path>.
        const probe = new PeerStorage(conf, noPeerSend);
        const oldKey = (file: string) => `${conf.name}-storage-${conf.baseDir}-file-stat-${probe.toLocalPath(file)}`;
        const size = (await Deno.stat(keep)).size;
        writeOldSettings(join(dir, "lsb-settings.sqlite"), [
            [OLD_MARKER, "1"],
            [oldKey("recorded.md"), `${mtime.getTime()}-${size}`],
            [oldKey("edited.md"), `${mtime.getTime()}-${size}`],
        ]);
        await initPeerState({ filePath: join(dir, "state.sqlite"), legacy: undefined });

        const delivered = new Set<string>();
        const peer = new PeerStorage(conf, (_source, path, _data: FileData | false) => {
            delivered.add(path);
            return Promise.resolve();
        });
        peer.sendLog = () => {};
        peer.debugLog = () => {};
        peer.normalLog = () => {};
        void peer.start();
        const deadline = Date.now() + 15_000;
        while (delivered.size < 2 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 50));
        await new Promise((resolve) => setTimeout(resolve, 500));
        await peer.stop();
        assert(delivered.has("edited.md"), "a file that differs from its record is sent");
        assert(delivered.has("unknown.md"), "a file without a record is sent");
        assert(!delivered.has("recorded.md"), "a file that matches its record is not sent");
        assertEquals(delivered.size, 2, "nothing else is sent");
    }));
