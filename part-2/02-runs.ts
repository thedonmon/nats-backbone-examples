import { connect } from "@nats-io/transport-node";
import { DeliverPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });
const jsm = await jetstreamManager(nc);
const js = jetstream(nc);

await jsm.streams.add({
  name: "CHAT",
  subjects: ["chat.>"],
  allow_msg_ttl: true,
  allow_msg_schedules: true,
});

// ---- responder side ----
async function startRun(conv: string, run: string, deadlineMs: number) {
  const base = `chat.${conv}.run.${run}`;
  await js.publish(`${base}.deadline`, JSON.stringify({ reason: "deadline" }), {
    schedule: {
      specification: new Date(Date.now() + deadlineMs),
      target: `${base}.timeout`,
    },
  });
  return base;
}

async function streamReply(base: string, text: string, finish: boolean) {
  let seq = 0;
  for (const token of text.split(" ")) {
    await js.publish(`${base}.chunk`, JSON.stringify({ seq: ++seq, token }), { ttl: "1h" });
  }
  if (finish) {
    await js.publish(`${base}.done`, JSON.stringify({ chunks: seq }), {
      cancelSchedule: { scheduleSubject: `${base}.deadline` },
    });
  }
}

// ---- client side: read a run, resuming after a given stream sequence ----
async function readRun(base: string, afterSeq = 0, limit = Infinity) {
  const c = await js.consumers.get("CHAT", {
    filter_subjects: [`${base}.chunk`, `${base}.done`],
    ...(afterSeq
      ? { deliver_policy: DeliverPolicy.StartSequence, opt_start_seq: afterSeq + 1 }
      : {}),
  });
  const tokens: string[] = [];
  let last = afterSeq;
  const iter = await c.consume();
  for await (const m of iter) {
    last = m.seq;
    if (m.subject.endsWith(".done")) break;
    tokens.push(m.json<{ token: string }>().token);
    if (tokens.length >= limit) break;
  }
  await iter.close();
  return { tokens, last };
}

const ok = await startRun("conv-42", "run-1", 3000);
await streamReply(ok, "NATS keeps the reply until the client catches up", true);

const first = await readRun(ok, 0, 4);
console.log("before disconnect:", first.tokens.join(" "), `(cursor ${first.last})`);
const rest = await readRun(ok, first.last);
console.log("after reconnect: ", rest.tokens.join(" "));

const stuck = await startRun("conv-42", "run-2", 2000);
await streamReply(stuck, "this model stalls mid", false);

await new Promise((r) => setTimeout(r, 3500));
const subjects = await jsm.streams.info("CHAT", { subjects_filter: "chat.*.run.*.timeout" });
console.log("timeouts fired:", Object.keys(subjects.state.subjects ?? {}));
const t = await jsm.streams.getMessage("CHAT", { last_by_subj: "chat.conv-42.run.run-2.timeout" });
console.log("timeout body:", t?.string());

await jsm.streams.delete("CHAT");
await nc.drain();
