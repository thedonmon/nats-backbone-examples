import { connect } from "@nats-io/transport-node";
import { Objm, StorageType } from "@nats-io/obj";
import { randomBytes } from "node:crypto";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });

const store = await new Objm(nc).create("models", {
  storage: StorageType.File,
  description: "Model artifacts",
});

const model = randomBytes(1024 * 1024); // stand-in for ./model-v2.3.bin
const info = await store.putBlob(
  { name: "classifier/v2.3", description: "Text classifier, trained 2026-01" },
  model,
);
console.log(`Stored ${info.name}: ${info.size} bytes in ${info.chunks} chunks`);
console.log(`Digest: ${info.digest}`);

const data = await store.getBlob("classifier/v2.3");
console.log("round trip matches:", data !== null && Buffer.compare(Buffer.from(data), model) === 0);

for (const obj of await store.list()) console.log(`${obj.name}: ${obj.size} bytes`);

await store.destroy();
await nc.drain();
