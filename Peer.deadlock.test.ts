// Two peers each see a change of the same path at the same moment: one has a new version of the
// file, the other has just lost it. Each delivers its change to the other one. Both deliveries
// must finish; earlier each held its own per-path lock while waiting for the other's, forever.
import { Peer } from "./Peer.ts";
import type { FileData, PeerStorageConf } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) throw new Error(`${message}\nactual=${actual}\nexpected=${expected}`);
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

class LinkedPeer extends Peer {
    other!: LinkedPeer;
    received: string[] = [];
    constructor(name: string) {
        const config: PeerStorageConf = { type: "storage", name, baseDir: "" };
        super(config, async () => {});
    }
    // A change which starts at this peer, as a watcher event does.
    originate(path: string, data: FileData | false, hubPath = path): Promise<boolean> {
        return this.withOriginLock(hubPath, () =>
            this.applyChange(path, data, async () => {
                await sleep(50); // long enough for the other peer's change to start too
                if (data === false) await this.other.delete(path);
                else await this.other.put(path, data);
                return true;
            }, { suppressPendingEcho: true }));
    }
    delete(path: string): Promise<boolean> {
        return this.applyChange(path, false, () => {
            this.received.push(`-${path}`);
            return Promise.resolve(true);
        });
    }
    put(path: string, data: FileData): Promise<boolean> {
        return this.applyChange(path, data, () => {
            this.received.push(`+${path}`);
            return Promise.resolve(true);
        });
    }
    get(): Promise<false | FileData> {
        return Promise.resolve(false);
    }
    start(): Promise<void> {
        return Promise.resolve();
    }
    stop(): Promise<void> {
        return Promise.resolve();
    }
}

function pair(): [LinkedPeer, LinkedPeer] {
    const a = new LinkedPeer("a");
    const b = new LinkedPeer("b");
    a.other = b;
    b.other = a;
    return [a, b];
}

const text = (value: string): FileData => ({ ctime: 1, mtime: 1, size: value.length, data: [value] });

async function finishes(work: Promise<unknown>, ms: number): Promise<boolean> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), ms);
    });
    try {
        return await Promise.race([work.then(() => true as const), timeout]);
    } finally {
        clearTimeout(timer);
    }
}

Deno.test({
    name: "a change at one peer and a deletion at the other, of the same path at the same time, both finish",
    // When this fails the two deliveries never settle, so the leak checks would only add noise.
    sanitizeOps: false,
    sanitizeResources: false,
    fn: async () => {
        const [a, b] = pair();
        const both = Promise.all([a.originate("note.md", text("new")), b.originate("note.md", false)]);
        assertEquals(await finishes(both, 3_000), true, "both deliveries finish");
        assertEquals(b.received.join(","), "+note.md", "the change reached the other peer");
        assertEquals(a.received.join(","), "-note.md", "the deletion reached the other peer");
    },
});

Deno.test({
    name: "two different contents of the same path, one from each peer at the same time, both finish",
    sanitizeOps: false,
    sanitizeResources: false,
    fn: async () => {
        const [a, b] = pair();
        const both = Promise.all([a.originate("note.md", text("from a")), b.originate("note.md", text("from b"))]);
        assertEquals(await finishes(both, 3_000), true, "both deliveries finish");
        assertEquals(a.received.join(",") + "|" + b.received.join(","), "+note.md|+note.md", "each reached the other peer");
    },
});

Deno.test({
    name: "the peers may name the same file with a leading slash, another case or another Unicode form",
    sanitizeOps: false,
    sanitizeResources: false,
    fn: async () => {
        const [a, b] = pair();
        const nfd = "Résumé.md".normalize("NFD");
        const both = Promise.all([
            a.originate("x", text("new"), "/Dir/" + nfd),
            b.originate("x", false, "dir/RÉSUMÉ.md"),
        ]);
        assertEquals(await finishes(both, 3_000), true, "both deliveries finish");
    },
});

Deno.test("changes of different paths are not made to wait for each other", async () => {
    const [a, b] = pair();
    const started = Date.now();
    await Promise.all(["1.md", "2.md", "3.md", "4.md"].map((path) => a.originate(path, text(path))));
    const elapsed = Date.now() - started;
    assertEquals(elapsed < 150, true, `four paths are delivered side by side (took ${elapsed} ms)`);
    assertEquals(b.received.length, 4, "all four arrived");
});

Deno.test("the record of handled contents holds a bulk move's worth of files", async () => {
    const [a] = pair();
    const data = text("same");
    for (let i = 0; i < 2_000; i++) await a.isRepeating(`bulk/${i}.md`, data);
    assertEquals(await a.isRepeating("bulk/0.md", data), true, "the first of 2,000 files is still remembered");
});
