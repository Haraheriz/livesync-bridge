// At most maxConcurrentSends files are read and sent at the same time; the others wait with
// only their path and are read when their turn comes.
import { join } from "@std/path";
import { PeerStorage } from "./PeerStorage.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
}
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function waitFor(condition: () => boolean, message: string, timeoutMs = 20_000) {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error(`Timed out while waiting: ${message}`);
        await sleep(10);
    }
}

function makePeer(baseDir: string, maxConcurrentSends?: number) {
    const config: PeerStorageConf = {
        type: "storage",
        name: `conc-${crypto.randomUUID()}`,
        baseDir,
        scanOfflineChanges: false,
        useChokidar: false,
        maxConcurrentSends,
    };
    const peer = new PeerStorage(config, () => Promise.resolve());
    // Keep file records in memory, without the deferred save timer of the real state.
    const settings = new Map<string, string>();
    peer.setSetting = (key, value) => {
        settings.set(key, value);
        return Promise.resolve();
    };
    peer.getSetting = (key) => Promise.resolve(settings.get(key) ?? null);
    peer.setSettingLater = (key, value) => void peer.setSetting(key, value);
    const notices: string[] = [];
    peer.normalLog = (message) => void notices.push(String(message));
    peer.debugLog = () => {};
    peer.sendLog = () => {};
    return { peer, notices };
}

async function vaultWith(n: number) {
    const dir = await Deno.makeTempDir({ prefix: "conc-" });
    for (let i = 0; i < n; i++) await Deno.writeTextFile(join(dir, `n${i}.md`), `note ${i}`);
    return dir;
}

Deno.test("no more than the limit are read or sent at once, and every file is sent", async () => {
    const dir = await vaultWith(200);
    try {
        const { peer } = makePeer(dir, 10);
        let reading = 0, inFlight = 0, maxInFlight = 0, maxHeld = 0;
        const sent = new Set<string>();
        const get = peer.get.bind(peer);
        peer.get = async (path, access) => {
            reading++;
            maxHeld = Math.max(maxHeld, reading);
            return await get(path, access);
        };
        peer.dispatchToHub = async (_source, path: string, _data: FileData | false) => {
            inFlight++;
            maxInFlight = Math.max(maxInFlight, inFlight);
            await sleep(10);
            inFlight--;
            reading--;
            sent.add(path);
        };
        // All 200 reported at once, as the watcher does when a large folder appears.
        for (let i = 0; i < 200; i++) await peer.dispatch(join(dir, `n${i}.md`));
        await waitFor(() => sent.size === 200, "every file is sent");
        assertEquals(maxInFlight <= 10, true, `at most 10 sent at once (was ${maxInFlight})`);
        assertEquals(maxHeld <= 10, true, `file contents read only for files whose turn came (held ${maxHeld})`);
        assertEquals(peer.sendQueueStatus().waiting, 0, "nothing left waiting");
        await peer.stop();
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
});

Deno.test("the limit defaults to 10 and a configured value is honoured", async () => {
    const dir = await vaultWith(40);
    try {
        for (const [configured, expected] of [[undefined, 10], [3, 3]] as const) {
            const { peer } = makePeer(dir, configured);
            let inFlight = 0, maxInFlight = 0, done = 0;
            peer.dispatchToHub = async () => {
                inFlight++;
                maxInFlight = Math.max(maxInFlight, inFlight);
                await sleep(30);
                inFlight--;
                done++;
            };
            for (let i = 0; i < 40; i++) await peer.dispatch(join(dir, `n${i}.md`));
            await waitFor(() => done === 40, "all files are sent");
            assertEquals(maxInFlight, expected, `the number of concurrent sends with maxConcurrentSends=${configured}`);
            await peer.stop();
        }
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
});

Deno.test("the content sent is the one on disk when the file's turn comes", async () => {
    const dir = await vaultWith(1);
    try {
        const { peer } = makePeer(dir, 1);
        const got: string[] = [];
        const firstBlocked = Promise.withResolvers<void>();
        await Deno.writeTextFile(join(dir, "blocker.md"), "b");
        peer.dispatchToHub = async (_source, path: string, data: FileData | false) => {
            if (path === "blocker.md") await firstBlocked.promise;
            got.push(`${path}=${data ? (data.data as string[]).join("") : "deleted"}`);
        };
        await peer.dispatch(join(dir, "blocker.md"));
        await sleep(400); // blocker holds the only slot
        await peer.dispatch(join(dir, "n0.md"));
        await sleep(400);
        await Deno.writeTextFile(join(dir, "n0.md"), "edited while waiting");
        firstBlocked.resolve();
        await waitFor(() => got.length === 2, "both files are sent");
        assertEquals(got.join(","), "blocker.md=b,n0.md=edited while waiting", "newest content sent");
        await peer.stop();
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
});

Deno.test("progress is logged while files are waiting", async () => {
    const dir = await vaultWith(30);
    try {
        const { peer, notices } = makePeer(dir, 1);
        let done = 0;
        peer.dispatchToHub = async () => {
            await sleep(20);
            done++;
        };
        for (let i = 0; i < 30; i++) await peer.dispatch(join(dir, `n${i}.md`));
        await sleep(400);
        // deno-lint-ignore no-explicit-any
        (peer as any).reportSendProgress();
        await waitFor(() => done === 30, "all files are sent");
        assertEquals(
            notices.some((n) => /Sending: \d+ file\(s\) handled in the last minute, \d+ waiting\./.test(n)),
            true,
            "progress line",
        );
        await peer.stop();
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
});
