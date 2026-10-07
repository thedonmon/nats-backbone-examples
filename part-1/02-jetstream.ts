import { connect } from "@nats-io/transport-node";
import { AckPolicy, DeliverPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });
const jsm = await jetstreamManager(nc);
const js = jetstream(nc);

await jsm.streams.add({
  name: "ORDERS",
  subjects: ["orders.*"],
  retention: "limits",
  max_msgs: 1_000_000,
  max_bytes: 1024 * 1024 * 1024,
  max_age: 7 * 24 * 60 * 60 * 1_000_000_000,
});

const ack = await js.publish("orders.created", JSON.stringify({
  orderId: "ORD-12345", customerId: "CUST-789", items: [{ sku: "WIDGET-01", quantity: 2 }], total: 49.99,
}));
console.log(`Stored in ${ack.stream} at sequence ${ack.seq}`);

for (let i = 0; i < 2; i++) {
  const a = await js.publish("orders.created", JSON.stringify({ orderId: "ORD-12346" }), { msgID: "ORD-12346" });
  console.log(`ORD-12346 attempt ${i + 1}: seq ${a.seq}, duplicate=${a.duplicate}`);
}

await jsm.consumers.add("ORDERS", {
  durable_name: "order-processor",
  ack_policy: AckPolicy.Explicit,
  deliver_policy: DeliverPolicy.All,
  filter_subject: "orders.created",
  max_ack_pending: 100,
});

const consumer = await js.consumers.get("ORDERS", "order-processor");
const messages = await consumer.consume();
let n = 0;
for await (const msg of messages) {
  const order = msg.json<{ orderId: string }>();
  console.log(`Processing order ${order.orderId}`);
  await msg.ackAck();
  if (++n === 2) break;
}

await jsm.streams.delete("ORDERS");
await nc.drain();
