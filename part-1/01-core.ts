import { connect } from "@nats-io/transport-node";

const nc = await connect({ servers: process.env.NATS_URL ?? "127.0.0.1:4222" });

// Pub/sub with a wildcard subscription
const sub = nc.subscribe("events.user.*", { max: 2 });
const received = (async () => {
  for await (const msg of sub) {
    console.log(`Received on ${msg.subject}:`, msg.string());
  }
})();

nc.publish("events.user.signup", JSON.stringify({ userId: "user-123", email: "dev@example.com", timestamp: Date.now() }));
nc.publish("events.user.login", JSON.stringify({ userId: "user-123" }));
await received;

// Request-reply with a queue group: two instances share the requests
const users: Record<string, { userId: string; name: string }> = {
  "user-123": { userId: "user-123", name: "Ada" },
};
for (const instance of ["a", "b"]) {
  const svc = nc.subscribe("api.users.get", { queue: "users" });
  (async () => {
    for await (const msg of svc) {
      const { userId } = msg.json<{ userId: string }>();
      msg.respond(JSON.stringify({ ...users[userId], servedBy: instance }));
    }
  })();
}

for (let i = 0; i < 4; i++) {
  const response = await nc.request("api.users.get", JSON.stringify({ userId: "user-123" }), { timeout: 5000 });
  console.log("User:", response.json());
}

await nc.drain();
