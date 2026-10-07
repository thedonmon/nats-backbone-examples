import { connect, headers } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });
const jsm = await jetstreamManager(nc);
const js = jetstream(nc);

// One counter stream per region, plus a global one that sources both.
for (const region of ["us", "eu"]) {
  await jsm.streams.add({
    name: `USAGE_${region.toUpperCase()}`,
    subjects: [`usage.${region}.>`],
    allow_msg_counter: true,
    allow_direct: true,
  });
}
await jsm.streams.add({
  name: "USAGE_GLOBAL",
  subjects: [],
  allow_msg_counter: true,
  allow_direct: true,
  sources: [
    { name: "USAGE_US", subject_transforms: [{ src: "usage.us.>", dest: "usage.all.>" }] },
    { name: "USAGE_EU", subject_transforms: [{ src: "usage.eu.>", dest: "usage.all.>" }] },
  ],
});

async function addTokens(region: string, tenant: string, n: number) {
  const h = headers();
  h.set("Nats-Incr", n >= 0 ? `+${n}` : `${n}`);
  await js.publish(`usage.${region}.${tenant}.tokens`, "", { headers: h });
}

async function read(stream: string, subject: string) {
  const m = await jsm.direct.getMessage(stream, { last_by_subj: subject });
  return m ? (m.json<{ val: string }>().val) : "0";
}

await Promise.all([
  addTokens("us", "acme", 1200),
  addTokens("us", "acme", 800),
  addTokens("eu", "acme", 450),
  addTokens("eu", "acme", -50), // refund
]);

await new Promise((r) => setTimeout(r, 500));
console.log("us:    ", await read("USAGE_US", "usage.us.acme.tokens"));
console.log("eu:    ", await read("USAGE_EU", "usage.eu.acme.tokens"));
console.log("global:", await read("USAGE_GLOBAL", "usage.all.acme.tokens"));

for (const s of ["USAGE_GLOBAL", "USAGE_US", "USAGE_EU"]) await jsm.streams.delete(s);
await nc.drain();
