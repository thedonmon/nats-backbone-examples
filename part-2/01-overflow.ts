import { connect } from "@nats-io/transport-node";
import { AckPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });
const jsm = await jetstreamManager(nc);
const js = jetstream(nc);

await jsm.streams.add({
  name: "INFERENCE",
  subjects: ["inference.jobs.>"],
  retention: "workqueue",
});

await jsm.consumers.add("INFERENCE", {
  durable_name: "gpu-workers",
  ack_policy: AckPolicy.Explicit,
  priority_policy: "overflow",
  priority_groups: ["gpu"],
});

// Seed a backlog.
for (let i = 0; i < 60; i++) {
  await js.publish(`inference.jobs.tenant-a`, JSON.stringify({ job: i }));
}

const consumer = await js.consumers.get("INFERENCE", "gpu-workers");

// Cheap on-prem GPU: always pulls.
async function onPrem() {
  let n = 0;
  const iter = await consumer.fetch({ max_messages: 10, expires: 2000, group: "gpu" });
  for await (const m of iter) { n++; m.ack(); }
  return n;
}

// Paid burst capacity: only gets work while 25+ jobs are waiting.
async function burst() {
  let n = 0;
  const iter = await consumer.fetch({ max_messages: 10, expires: 2000, group: "gpu", min_pending: 25 });
  for await (const m of iter) { n++; m.ack(); }
  return n;
}

for (let round = 0; round < 6; round++) {
  const [a, b] = await Promise.all([onPrem(), burst()]);
  const info = await consumer.info();
  console.log(`round ${round}: on-prem=${a} burst=${b} pending=${info.num_pending}`);
}

await jsm.streams.delete("INFERENCE");
await nc.drain();
