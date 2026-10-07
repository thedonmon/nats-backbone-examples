import { connect } from "@nats-io/transport-node";
import { AckPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });
const jsm = await jetstreamManager(nc);
const js = jetstream(nc);

await jsm.streams.add({ name: "EMBED", subjects: ["embed.>"] });
await jsm.consumers.add("EMBED", { durable_name: "indexer", ack_policy: AckPolicy.Explicit });
for (let i = 0; i < 10; i++) await js.publish("embed.doc", String(i));

// Pause the indexer for a deploy window. Messages keep landing in the stream.
const paused = await jsm.consumers.pause("EMBED", "indexer", new Date(Date.now() + 15 * 60_000));
console.log("paused until:", paused.pause_until);
await jsm.consumers.resume("EMBED", "indexer");

// Process everything, then discover a bug and reprocess from sequence 4.
const c = await js.consumers.get("EMBED", "indexer");
const batch = await c.fetch({ max_messages: 10, expires: 1000 });
for await (const m of batch) m.ack();
console.log("pending after run:", (await c.info()).num_pending);
const reset = await jsm.consumers.reset("EMBED", "indexer", 4);
console.log("pending after reset:", reset.num_pending);

await jsm.streams.delete("EMBED");
await nc.drain();
