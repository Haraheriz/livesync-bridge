import { join } from "@std/path";
import { PeerStorage } from "./PeerStorage.ts";
import type { FileChangeContext } from "./Peer.ts";
import type { FileData } from "./types.ts";

// Folder notifications on a peer with NFC path identity: the files found below the folder are
// sent (or deleted) under the spelling that is on disk, and a confirmed rename keeps its source.

function assert(value: unknown, message: string): asserts value {
    if (!value) throw new Error(message);
}
function equal(actual: unknown, expected: unknown, message: string) {
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
        throw new Error(`${message}: expected=${JSON.stringify(expected)}, actual=${JSON.stringify(actual)}`);
    }
}
const sleep = (milliseconds: number) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
async function waitFor(condition: () => boolean, message: string) {
    const deadline = Date.now() + 5000;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error(`Timed out waiting for: ${message}`);
        await sleep(10);
    }
}

type Delivered = { path: string; data: FileData | false; context?: FileChangeContext };

function makePeer(baseDir: string) {
    const delivered: Delivered[] = [];
    const peer = new PeerStorage(
        {
            type: "storage",
            name: crypto.randomUUID(),
            baseDir,
            pathNormalizationVersion: 1,
            handleFilenameCaseSensitive: true,
            ignore: ["**/private"],
        },
        (_source, path, data, context) => {
            delivered.push({ path, data, context });
            return Promise.resolve();
        },
    );
    peer.normalLog = () => {};
    peer.receiveLog = () => {};
    peer.sendLog = () => {};
    // Keep the file records in memory, without the deferred save timer of the real state.
    const settings = new Map<string, string>();
    peer.setSetting = (key, value) => {
        settings.set(key, value);
        return Promise.resolve();
    };
    peer.getSetting = (key) => Promise.resolve(settings.get(key) ?? null);
    peer.setSettingLater = (key, value) => void peer.setSetting(key, value);
    return { peer, delivered };
}

Deno.test("NFC folder creation sends only the files below it, once, under their native spelling", async () => {
    const root = await Deno.makeTempDir({ prefix: "bridge-nfc-folder-" });
    const { peer, delivered } = makePeer(root);
    const folder = "Café".normalize("NFD");
    const leaf = "Résumé.md".normalize("NFD");
    try {
        await Deno.mkdir(join(root, folder, "private"), { recursive: true });
        await Deno.mkdir(join(root, folder, "deep"));
        await Deno.writeTextFile(join(root, folder, leaf), "top");
        await Deno.writeTextFile(join(root, folder, "deep", "note.md"), "nested");
        await Deno.writeTextFile(join(root, folder, "private", "secret.md"), "ignored");
        // The folder is reported without its files; the same notification can arrive twice.
        peer.processFile({ kind: "create", paths: [join(root, folder)] });
        peer.processFile({ kind: "create", paths: [join(root, folder)] });
        await waitFor(() => delivered.length >= 2, "both files below the folder");
        await sleep(600);
        equal(
            delivered.map((entry) => entry.path).sort(),
            [`${folder}/${leaf}`, `${folder}/deep/note.md`].sort(),
            "Only the files outside the ignored folder are sent, once each, with the on-disk spelling",
        );
        assert(delivered.every((entry) => entry.data !== false), "A new folder must not delete anything");
    } finally {
        await peer.stop();
        await Deno.remove(root, { recursive: true });
    }
});

Deno.test("NFC confirmed folder rename moves the files and removal of the folder deletes them", async () => {
    const root = await Deno.makeTempDir({ prefix: "bridge-nfc-rename-" });
    const { peer, delivered } = makePeer(root);
    const before = join(root, "before");
    const after = join(root, "Après".normalize("NFD"));
    try {
        await Deno.mkdir(join(before, "deep"), { recursive: true });
        await Deno.writeTextFile(join(before, "deep", "note.md"), "body");
        await peer.writeFileStat("before/deep/note.md");
        await Deno.rename(before, after);
        peer.processFile({ kind: "rename", paths: [before, after] });
        await waitFor(() => delivered.length >= 1, "the renamed file");
        await sleep(600);
        // The move itself, and the old path's removal which the destination ignores once it
        // has handled the move (see Peer.deleteFrom).
        const moved = delivered.filter((entry) => entry.data !== false);
        equal(moved.length, 1, "The file is sent once");
        equal(moved[0].path, `${"Après".normalize("NFD")}/deep/note.md`, "The new path is the on-disk spelling");
        equal(moved[0].context, { previousPath: "before/deep/note.md" }, "The previous path travels with the file");
        equal(
            delivered.filter((entry) => entry.data === false).map((entry) => entry.path),
            ["before/deep/note.md"],
            "Only the old path is reported as removed",
        );

        delivered.length = 0;
        await Deno.remove(after, { recursive: true });
        peer.processFile({ kind: "remove", paths: [after] });
        await waitFor(() => delivered.length >= 1, "the deletion of the removed folder's file");
        equal(
            delivered.map((entry) => [entry.path, entry.data]),
            [[`${"Après".normalize("NFD")}/deep/note.md`, false]],
            "A removed folder deletes the files known below it",
        );
    } finally {
        await peer.stop();
        await Deno.remove(root, { recursive: true });
    }
});
