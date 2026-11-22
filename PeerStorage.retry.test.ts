// A change which could not be delivered is tried again 30, 60, 90, 120 and 150 seconds later
// (n times sendRetryIntervalSeconds, which the tests make small), reading the file as it is then.
// After the fifth retry it is left unrecorded for the periodic rescan. The waiting takes no send
// slot, is not stacked on by a rescan, and ends with stop().
import { join } from "@std/path";
import { LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";
import { Hub } from "./Hub.ts";
import type { Peer } from "./Peer.ts";
import { PeerStorage } from "./PeerStorage.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
async function waitFor(condition: () => boolean, message: string, timeoutMs = 15_000) {
    const deadline = Date.now() + timeoutMs;
    while (!condition()) {
        if (Date.now() > deadline) throw new Error(`Timed out while waiting: ${message}`);
        await sleep(10);
    }
}
const text = (data: FileData | false) =>
    data === false ? "" : Array.isArray(data.data) ? data.data.join("") : new TextDecoder().decode(data.data);

function makePeer(dir: string, extra: Partial<PeerStorageConf> = {}) {
    const config: PeerStorageConf = {
        type: "storage",
        name: `retry-${crypto.randomUUID()}`,
        baseDir: dir,
        scanOfflineChanges: false,
        useChokidar: false,
        rescanIntervalMinutes: 0,
        sendRetryIntervalSeconds: 0.02,
        ...extra,
    };
    const peer = new PeerStorage(config, () => Promise.resolve());
    const settings = new Map<string, string>();
    peer.setSetting = (key, value) => {
        settings.set(key, value);
        return Promise.resolve();
    };
    peer.getSetting = (key) => Promise.resolve(settings.get(key) ?? null);
    peer.setSettingLater = (key, value) => void peer.setSetting(key, value);
    const notices: string[] = [];
    peer.normalLog = (message, level) => {
        if (level === LOG_LEVEL_NOTICE) notices.push(message);
    };
    peer.debugLog = () => {};
    peer.sendLog = () => {};
    peer.receiveLog = () => {};
    return { peer, notices };
}

type Call = { path: string; content: string; deleted: boolean; at: number };
// A hub which fails while `failures` is above 0, and records what it was asked to deliver.
function failingHub(peer: PeerStorage, failures: { remaining: number }) {
    const calls: Call[] = [];
    peer.dispatchToHub = (_source, path, data) => {
        calls.push({ path, content: text(data), deleted: data === false, at: Date.now() });
        if (failures.remaining > 0) {
            failures.remaining--;
            return Promise.reject(new Error("CouchDB is down"));
        }
        return Promise.resolve();
    };
    return calls;
}

async function withVault(body: (dir: string) => Promise<void>) {
    const dir = await Deno.makeTempDir({ prefix: "retry-" });
    try {
        await body(dir);
    } finally {
        await Deno.remove(dir, { recursive: true });
    }
}

Deno.test("a failed send is retried after n times the interval, with the latest content, and is recorded once it succeeds", async () => {
    await withVault(async (dir) => {
        const { peer } = makePeer(dir, { sendRetryIntervalSeconds: 0.2 });
        const calls = failingHub(peer, { remaining: 2 });
        try {
            await Deno.writeTextFile(join(dir, "a.md"), "first");
            await peer.dispatch(join(dir, "a.md"));
            await waitFor(() => calls.length === 1, "the first attempt");
            await sleep(100);
            assertEquals(await peer.isChanged("a.md"), true, "a failed send is not recorded as sent");
            assertEquals(peer.sendRetriesWaiting(), 1, "a retry is waiting");

            // The file is edited while the retry waits: the retry sends what is there then.
            await Deno.writeTextFile(join(dir, "a.md"), "second");
            await waitFor(() => calls.length === 3, "the second retry");
            assertEquals(calls.map((call) => call.content).join(","), "first,second,second", "each attempt reads the file again");
            // The wait before the retry n is n * 0.2 s, on top of the 0.25 s the peer always waits.
            assertEquals(calls[1].at - calls[0].at >= 0.2 * 1000 + 200, true, `retry 1 waited: ${calls[1].at - calls[0].at} ms`);
            assertEquals(calls[2].at - calls[1].at >= 0.4 * 1000 + 200, true, `retry 2 waited longer: ${calls[2].at - calls[1].at} ms`);
            await waitFor(() => peer.sendRetriesWaiting() === 0, "no retry is waiting after the success");
            await sleep(50);
            assertEquals(await peer.isChanged("a.md"), false, "recorded as sent once it succeeded");
        } finally {
            await peer.stop();
        }
    });
});

Deno.test("after five failed retries a notice is given, the file stays unrecorded, and the rescan sends it", async () => {
    await withVault(async (dir) => {
        const { peer, notices } = makePeer(dir);
        const failures = { remaining: 100 };
        const calls = failingHub(peer, failures);
        try {
            await Deno.writeTextFile(join(dir, "c.md"), "c");
            await peer.dispatch(join(dir, "c.md"));
            await waitFor(() => calls.length === 6, "the first attempt and five retries");
            await waitFor(() => notices.some((notice) => notice.includes("5 retries")), "the notice");
            await sleep(500);
            assertEquals(calls.length, 6, "no further retry");
            assertEquals(peer.sendRetriesWaiting(), 0, "nothing is waiting");
            assertEquals(await peer.isChanged("c.md"), true, "still unrecorded, so a rescan will send it");

            failures.remaining = 0;
            assertEquals(await peer.rescan(dir, "periodic"), 1, "the rescan queues it again");
            await waitFor(() => calls.length === 7, "the rescan sends it");
            await sleep(100);
            assertEquals(await peer.isChanged("c.md"), false, "recorded after the rescan's send");
        } finally {
            await peer.stop();
        }
    });
});

Deno.test("a failed deletion is retried the same way, and not sent when the file exists again", async () => {
    await withVault(async (dir) => {
        const { peer } = makePeer(dir);
        const calls = failingHub(peer, { remaining: 2 });
        try {
            // dispatchDeleted() reports the failure to its caller (the watcher), and the retry follows.
            await peer.dispatchDeleted(join(dir, "gone.md")).catch(() => {});
            await waitFor(() => calls.length === 3, "the deletion is tried three times");
            assertEquals(calls.every((call) => call.deleted && call.path === "gone.md"), true, "all three are the deletion");
            await waitFor(() => peer.sendRetriesWaiting() === 0, "settled");

            // A deletion which fails, and a file which comes back before the retry.
            const second = failingHub(peer, { remaining: 1 });
            await peer.dispatchDeleted(join(dir, "back.md")).catch(() => {});
            await waitFor(() => second.length === 1 && peer.sendRetriesWaiting() === 1, "the deletion failed and waits");
            await Deno.writeTextFile(join(dir, "back.md"), "back");
            await waitFor(() => peer.sendRetriesWaiting() === 0, "the retry has run");
            await sleep(400);
            assertEquals(second.length, 1, "the deletion is not sent for a file which exists again");
        } finally {
            await peer.stop();
        }
    });
});

Deno.test("a file waiting for its retry holds no send slot", async () => {
    await withVault(async (dir) => {
        const { peer } = makePeer(dir, { maxConcurrentSends: 1, sendRetryIntervalSeconds: 30 });
        const calls: string[] = [];
        peer.dispatchToHub = (_source, path) => {
            calls.push(path);
            return path === "stuck.md" ? Promise.reject(new Error("down")) : Promise.resolve();
        };
        try {
            await Deno.writeTextFile(join(dir, "stuck.md"), "s");
            await peer.dispatch(join(dir, "stuck.md"));
            await waitFor(() => peer.sendRetriesWaiting() === 1, "the retry of stuck.md waits");
            for (let i = 0; i < 3; i++) {
                await Deno.writeTextFile(join(dir, `other${i}.md`), `o${i}`);
                await peer.dispatch(join(dir, `other${i}.md`));
            }
            await waitFor(() => calls.filter((path) => path.startsWith("other")).length === 3, "the others are sent while one waits");
            assertEquals(peer.sendQueueStatus().waiting, 0, "nothing is waiting for the slot");
        } finally {
            await peer.stop();
        }
        assertEquals(peer.sendRetriesWaiting(), 0, "stop() cancelled the retry");
    });
});

Deno.test("a rescan does not stack another send on a file which waits for its retry", async () => {
    await withVault(async (dir) => {
        const { peer } = makePeer(dir, { sendRetryIntervalSeconds: 30 });
        const calls = failingHub(peer, { remaining: 1 });
        try {
            await Deno.writeTextFile(join(dir, "w.md"), "w");
            await peer.dispatch(join(dir, "w.md"));
            await waitFor(() => peer.sendRetriesWaiting() === 1, "waiting for the retry");
            assertEquals(peer.isSendPending("w.md"), true, "a waiting retry counts as pending");
            assertEquals(await peer.rescan(dir, "periodic"), 0, "the rescan queues nothing for it");
            await sleep(500);
            assertEquals(calls.length, 1, "and nothing was sent");
        } finally {
            await peer.stop();
        }
    });
});

Deno.test("stop() cancels the waiting retries, and nothing is sent afterwards", async () => {
    await withVault(async (dir) => {
        const { peer } = makePeer(dir, { sendRetryIntervalSeconds: 0.1 });
        const calls = failingHub(peer, { remaining: 100 });
        await Deno.writeTextFile(join(dir, "s.md"), "s");
        await peer.dispatch(join(dir, "s.md"));
        await waitFor(() => peer.sendRetriesWaiting() === 1, "waiting for the retry");
        await peer.stop();
        assertEquals(peer.sendRetriesWaiting(), 0, "the timer is gone");
        const made = calls.length;
        await sleep(800);
        assertEquals(calls.length, made, "no retry after stop()");
    });
});

Deno.test("a new failure for a path restarts its retries instead of adding a second timer", async () => {
    await withVault(async (dir) => {
        const { peer } = makePeer(dir, { sendRetryIntervalSeconds: 30 });
        const calls = failingHub(peer, { remaining: 100 });
        try {
            await Deno.writeTextFile(join(dir, "n.md"), "one");
            await peer.dispatch(join(dir, "n.md"));
            await waitFor(() => calls.length === 1, "the first failure");
            await Deno.writeTextFile(join(dir, "n.md"), "two");
            await peer.dispatch(join(dir, "n.md"));
            await waitFor(() => calls.length === 2, "the second failure");
            await sleep(100);
            assertEquals(peer.sendRetriesWaiting(), 1, "one waiting retry for the path");
        } finally {
            await peer.stop();
        }
    });
});

Deno.test("sendRetryIntervalSeconds 0 turns the retries off", async () => {
    await withVault(async (dir) => {
        const { peer } = makePeer(dir, { sendRetryIntervalSeconds: 0 });
        const calls = failingHub(peer, { remaining: 100 });
        try {
            await Deno.writeTextFile(join(dir, "z.md"), "z");
            await peer.dispatch(join(dir, "z.md"));
            await waitFor(() => calls.length === 1, "the failure");
            await sleep(300);
            assertEquals(peer.sendRetriesWaiting(), 0, "no retry");
            assertEquals(calls.length, 1, "only the one attempt");
        } finally {
            await peer.stop();
        }
    });
});

Deno.test("with the hub, every peer is tried at each attempt and the source retries until all accept", async () => {
    await withVault(async (dir) => {
        const { peer: source } = makePeer(dir);
        const hub = new Hub({ peers: [] });
        const received: Record<string, string[]> = { bad: [], good: [] };
        let badFailures = 2;
        const fake = (name: string) => ({
            config: { name, group: "" },
            cache: { clear: () => {} },
            put: (_path: string, data: FileData) => {
                received[name].push(text(data));
                if (name === "bad" && badFailures-- > 0) return Promise.reject(new Error("bad is down"));
                return Promise.resolve(true);
            },
            delete: () => Promise.resolve(true),
        }) as unknown as Peer;
        hub.peers = [source, fake("bad"), fake("good")];
        source.dispatchToHub = hub.dispatch.bind(hub);
        try {
            await Deno.writeTextFile(join(dir, "h.md"), "hub");
            await source.dispatch(join(dir, "h.md"));
            await waitFor(() => received.bad.length === 3, "the failing peer is tried three times");
            assertEquals(received.good.length >= 3, true, "the peer after it was tried every time, not skipped");
            await waitFor(() => source.sendRetriesWaiting() === 0, "no retry is left");
            await sleep(50);
            assertEquals(await source.isChanged("h.md"), false, "recorded once every peer accepted");
        } finally {
            await source.stop();
        }
    });
});
