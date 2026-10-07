import { connect } from "@nats-io/transport-node";
import { jetstream } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });
const js = jetstream(nc);
const kv = await new Kvm(nc).create("leases", { markerTTL: 60_000 });

// Take the lease only if nobody holds it. The key expires on its own.
async function acquire(conv: string, worker: string) {
  try {
    return await kv.create(conv, worker, "3s");
  } catch {
    return 0;
  }
}

// Extend the lease, but only if we still hold the revision we think we hold.
// The KV client has no "update with TTL", so publish to the bucket's subject.
async function renew(conv: string, worker: string, revision: number) {
  const ack = await js.publish(`$KV.leases.${conv}`, worker, {
    ttl: "3s",
    expect: { lastSubjectSequence: revision },
  });
  return ack.seq;
}

const watch = await kv.watch();
(async () => {
  for await (const e of watch) {
    console.log(`  watch: ${e.key} ${e.operation} ${e.operation === "PUT" ? e.string() : ""}`);
  }
})();

let rev = await acquire("conv-42", "worker-a");
console.log("worker-a acquired at rev", rev);
console.log("worker-b acquire:", (await acquire("conv-42", "worker-b")) || "refused");

rev = await renew("conv-42", "worker-a", rev);
console.log("worker-a renewed, rev", rev);

console.log("worker-a stops renewing (crash)...");
await new Promise((r) => setTimeout(r, 4500));

console.log("worker-b acquire:", (await acquire("conv-42", "worker-b")) || "refused");

watch.stop();
await kv.destroy();
await nc.drain();
