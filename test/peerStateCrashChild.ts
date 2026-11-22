// Child process of the crash tests in PeerState.test.ts: scan a vault, then exit
// without flushing or disposing, as a power cut would leave it.
//   deno run -A test/peerStateCrashChild.ts VAULT STATE_FILE MODE FILES
// MODE "none":  nothing is ever flushed, so no record reaches the file.
// MODE "late":  the first scan is flushed; files then edited are sent but their new records are lost.
import { join } from "@std/path";
import { getPeerState, initPeerState } from "../PeerState.ts";
import { PeerStorage } from "../PeerStorage.ts";

const [vault, stateFile, mode, files] = Deno.args;
const total = Number(files);
await initPeerState({ filePath: stateFile, legacy: undefined, queue: { delayMs: 3_600_000 } });
const delivered = new Set<string>();
const peer = new PeerStorage({ type: "storage", name: "crash", baseDir: vault + "/", scanOfflineChanges: true }, (_source, path) => {
    delivered.add(path);
    return Promise.resolve();
});
peer.sendLog = () => {};
peer.debugLog = () => {};
peer.normalLog = () => {};
async function until(done: () => boolean) {
    const deadline = Date.now() + 30_000;
    while (!done() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 50));
    return done();
}
void peer.start();
let ok = await until(() => delivered.size >= total);
if (ok && mode === "late") {
    await getPeerState().flush();
    delivered.clear();
    for (let i = 0; i < 5; i++) await Deno.writeTextFile(join(vault, `n${i}.md`), `edited note ${i} with a different size`);
    ok = await until(() => delivered.size >= 5);
}
// Let the last queue operations run, but never flush or dispose.
await new Promise((resolve) => setTimeout(resolve, 300));
Deno.exit(ok ? 0 : 3);
