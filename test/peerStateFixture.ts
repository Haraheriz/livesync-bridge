import { join } from "@std/path";
import { closePeerState, initPeerState, type PeerState } from "../PeerState.ts";

// Open a throw-away peer state file for tests that exercise the real
// Peer.getSetting/setSetting. The directory is removed when the process exits.
export async function installTempPeerState(prefix = "lsb-peer-state-"): Promise<PeerState> {
    const dir = Deno.makeTempDirSync({ prefix });
    globalThis.addEventListener("unload", () => {
        try {
            Deno.removeSync(dir, { recursive: true });
        } catch {
            // Best effort: the OS temp directory is cleaned up eventually.
        }
    });
    return await initPeerState({ filePath: join(dir, "state.sqlite"), legacy: undefined });
}

export { closePeerState };
