# nats-backbone-examples

Runnable code for two posts about using NATS in place of Kafka, Redis, and S3:

- **[Part 1](https://dev.to/thedonmon/beyond-kafka-and-redis-a-practical-guide-to-nats-as-your-unified-cloud-native-backbone-4g86)**: Core NATS, JetStream, the Key-Value store, and the Object Store.
- **Part 2**: an AI chat backend built on features from NATS 2.11 to 2.15.

Every script runs with nats.js 3.4. Part 2 needs nats-server 2.14 or later, and the output in the post comes from 2.15.0.

## Run

Start a server with JetStream:

```bash
brew install nats-server
nats-server -js
```

Then pick a part:

```bash
cd part-2
npm install
npx tsx 01-overflow.ts
```

Scripts connect to `127.0.0.1:4222`. Set `NATS_URL` to use another server. Each script creates the streams and buckets it needs and deletes them before it exits.

## Part 1

| Script | Shows |
|---|---|
| `01-core.ts` | Wildcard pub/sub, and request-reply across a queue group |
| `02-jetstream.ts` | A limits stream, publish deduplication with `msgID`, and a durable consumer with `ackAck()` |
| `03-kv.ts` | Bucket TTL in milliseconds, watchers, optimistic locking with `kv.update()`, and key rules |
| `04-object-store.ts` | Storing, reading back, and listing an object |

## Part 2

| Script | Feature | Server |
|---|---|---|
| `01-overflow.ts` | Priority groups with the `overflow` policy: burst workers only pull when the backlog is large | 2.11 |
| `02-runs.ts` | Streamed replies with per-message TTL, resume from a cursor, and run deadlines with `@at` schedules that are cancelled on completion | 2.14 |
| `03-turns.ts` | Atomic batch publish, with a sequence check so only one of two concurrent writers commits | 2.12 |
| `04-counters.ts` | Distributed counters per region, aggregated into a global counter with stream sources | 2.12 |
| `05-leases.ts` | KV leases with per-key TTL, renewal, and expiry markers | 2.11 |
| `06-ops.ts` | Consumer pause and consumer reset | 2.14 |

Run `npm run typecheck` in either folder to typecheck the scripts.
