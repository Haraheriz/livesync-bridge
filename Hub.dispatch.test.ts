// Hub.dispatch tries every peer of the group, then reports the failures to the source.
import { Hub } from "./Hub.ts";
import type { Peer } from "./Peer.ts";
import type { FileData } from "./types.ts";

function assertEquals<T>(actual: T, expected: T, message: string) {
    if (actual !== expected) {
        throw new Error(`${message}\nactual=${String(actual)}\nexpected=${String(expected)}`);
    }
}

const file: FileData = { ctime: 1, mtime: 2, size: 5, data: ["hello"] };

function fakePeer(name: string, group: string, calls: string[], failure?: Error) {
    const act = (op: string) => {
        calls.push(`${name}:${op}`);
        return failure ? Promise.reject(failure) : Promise.resolve(true);
    };
    let cleared = 0;
    const peer = {
        config: { name, group },
        cache: { clear: () => void cleared++ },
        put: () => act("put"),
        delete: () => act("delete"),
        deleteFrom: () => act("deleteFrom"),
        rename: () => act("rename"),
    } as unknown as Peer;
    return { peer, cleared: () => cleared };
}

async function dispatchAndCatch(hub: Hub, source: Peer, data: FileData | false, context?: { previousPath: string }) {
    try {
        await hub.dispatch(source, "x.md", data, context);
    } catch (error) {
        return error;
    }
    return undefined;
}

Deno.test("Hub.dispatch delivers to the peers after a failing one, then reports the failure", async () => {
    const hub = new Hub({ peers: [] });
    const calls: string[] = [];
    const source = fakePeer("src", "g", calls);
    const bad = fakePeer("bad", "g", calls, new Error("bad is down"));
    const good = fakePeer("good", "g", calls);
    hub.peers = [source.peer, bad.peer, good.peer];

    const error = await dispatchAndCatch(hub, source.peer, file);

    assertEquals(calls.join(","), "bad:put,good:put", "every other peer of the group was tried");
    assertEquals((error as Error)?.message, "bad is down", "a single failure is reported as it is");
    assertEquals(bad.cleared(), 1, "the failed peer's handled records are cleared");
    assertEquals(good.cleared(), 0, "a peer which succeeded keeps its records");
});

Deno.test("Hub.dispatch reports several failures together", async () => {
    const hub = new Hub({ peers: [] });
    const calls: string[] = [];
    const source = fakePeer("src", "g", calls);
    hub.peers = [
        source.peer,
        fakePeer("bad1", "g", calls, new Error("one")).peer,
        fakePeer("good", "g", calls).peer,
        fakePeer("bad2", "g", calls, new Error("two")).peer,
    ];

    const error = await dispatchAndCatch(hub, source.peer, file);

    assertEquals(calls.join(","), "bad1:put,good:put,bad2:put", "every peer was tried");
    assertEquals(error instanceof AggregateError, true, "several failures become one AggregateError");
    assertEquals((error as AggregateError).errors.map((e: Error) => e.message).join(","), "one,two", "both failures are kept");
});

Deno.test("Hub.dispatch does not fail when every peer accepts, and skips other groups", async () => {
    const hub = new Hub({ peers: [] });
    const calls: string[] = [];
    const source = fakePeer("src", "g", calls);
    hub.peers = [
        source.peer,
        fakePeer("same", "g", calls).peer,
        fakePeer("other", "h", calls, new Error("not my group")).peer,
    ];

    assertEquals(await dispatchAndCatch(hub, source.peer, file), undefined, "no failure");
    assertEquals(calls.join(","), "same:put", "a peer of another group is not contacted");
});

Deno.test("Hub.dispatch applies the same rule to deletions and renames", async () => {
    const hub = new Hub({ peers: [] });
    const calls: string[] = [];
    const source = fakePeer("src", "", calls);
    hub.peers = [
        source.peer,
        fakePeer("bad", "", calls, new Error("down")).peer,
        fakePeer("good", "", calls).peer,
    ];

    const deletion = await dispatchAndCatch(hub, source.peer, false);
    const rename = await dispatchAndCatch(hub, source.peer, file, { previousPath: "old.md" });

    assertEquals(calls.join(","), "bad:deleteFrom,good:deleteFrom,bad:rename,good:rename", "all peers tried for both");
    assertEquals((deletion as Error)?.message, "down", "the deletion failure is reported");
    assertEquals((rename as Error)?.message, "down", "the rename failure is reported");
});
