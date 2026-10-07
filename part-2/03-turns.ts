import { connect } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });
const jsm = await jetstreamManager(nc);
const js = jetstream(nc);

await jsm.streams.add({
  name: "HISTORY",
  subjects: ["history.>"],
  allow_atomic: true,
});

type Entry = { role: "user" | "assistant" | "tool"; content: string };

// Append one full turn, but only if nobody else wrote to the conversation
// since we last read it.
async function appendTurn(conv: string, expectedLastSeq: number, entries: Entry[]) {
  const subject = `history.${conv}`;
  const [first, ...rest] = entries;
  const last = rest.pop()!;
  const batch = await js.startBatch(subject, JSON.stringify(first), {
    expect: { lastSubjectSequence: expectedLastSeq },
  });
  for (const e of rest) batch.add(subject, JSON.stringify(e));
  return batch.commit(subject, JSON.stringify(last));
}

const ack = await appendTurn("conv-42", 0, [
  { role: "user", content: "What's the weather in Lisbon?" },
  { role: "tool", content: '{"call":"weather","city":"Lisbon"}' },
  { role: "tool", content: '{"temp_c":24}' },
  { role: "assistant", content: "24°C and sunny." },
]);
console.log(`turn 1 committed: batch of ${ack.count}, last seq ${ack.seq}`);

// Two agents both read seq 4 and try to write the next turn.
const results = await Promise.allSettled([
  appendTurn("conv-42", ack.seq, [
    { role: "user", content: "And tomorrow?" },
    { role: "assistant", content: "Rain after noon." },
  ]),
  appendTurn("conv-42", ack.seq, [
    { role: "user", content: "And tomorrow?" },
    { role: "assistant", content: "Clear skies." },
  ]),
]);
for (const r of results) {
  console.log(r.status === "fulfilled" ? `committed at ${r.value.seq}` : `rejected: ${(r.reason as Error).message}`);
}

const info = await jsm.streams.info("HISTORY");
console.log("messages stored:", info.state.messages);

await jsm.streams.delete("HISTORY");
await nc.drain();
