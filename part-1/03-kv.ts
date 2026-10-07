import { connect } from "@nats-io/transport-node";
import { Kvm } from "@nats-io/kv";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });

const kv = await new Kvm(nc).create("sessions", {
  history: 5,
  ttl: 60 * 60 * 1000,
});
const status = await kv.status();
console.log("bucket ttl (ms):", status.ttl);

const watch = await kv.watch({ key: "session.>" });
(async () => {
  for await (const e of watch) console.log(`  watch: ${e.key} ${e.operation} rev ${e.revision}`);
})();

await kv.put("session.user-123", JSON.stringify({ userId: "user-123", role: "admin", lastActivity: Date.now() }));
const entry = await kv.get("session.user-123");
console.log("Session:", entry?.string());
console.log("Revision:", entry?.revision);

// Someone else writes first, so our update with the old revision fails
await kv.put("session.user-123", JSON.stringify({ role: "editor" }));
try {
  await kv.update("session.user-123", JSON.stringify({ role: "viewer" }), entry!.revision);
  console.log("update succeeded");
} catch {
  console.log("Someone else changed this session first");
}

try {
  await kv.put("session:user-123", "x");
} catch (err) {
  console.log("colon key:", (err as Error).message);
}

await new Promise((r) => setTimeout(r, 200));
watch.stop();
await kv.destroy();
await nc.drain();
