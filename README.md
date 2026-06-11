# Distributed SMS Service

A polyglot, event-driven microservices platform for sending, auditing, and storing SMS messages — built around a **transactional outbox** for durable acceptance and **end-to-end idempotency** for exactly-once delivery.

A request is accepted and acknowledged in milliseconds; the actual vendor call and event publication happen asynchronously in the background. The two services never call each other directly — Kafka is the only link between them.

---

## Key Features

- **Transactional outbox** — `POST /send` durably records the request and returns `202 Accepted`; a background worker does the real work. A Kafka or vendor outage never blocks or fails the request path.
- **Exactly-once to the recipient** — a two-step state machine plus an idempotency key passed to the vendor guarantee no double-send on retry or crash.
- **Exactly-once storage** — the consumer upserts by `eventId`, so Kafka's at-least-once delivery still yields exactly one MongoDB document per request.
- **Exponential backoff + dead-letter** — failures retry with growing delays; genuinely stuck messages are quarantined as `FAILED` and can be replayed by an admin.
- **Blocklist** — numbers can be blocked/unblocked/checked via Redis; blocked numbers are rejected before they ever reach the outbox.
- **API-key–protected admin surface** — block/unblock and outbox endpoints require an `X-API-Key`; the demo gateway injects it so the secret never lives in the browser.
- **Live demo UI** — a single-page console with a real-time Outbox Monitor, blocklist manager, and message history.

---

## Architecture Overview

```
  Client / UI            ┌─────────────── JAVA SMS SENDER  :8080 ───────────────┐
   (browser)             │  Controller ─► Redis block check                     │
       │  ──HTTP──►       │       │                                              │
       │                 │       ▼      ┌──── two-step worker ────┐             │
       │                 │   Outbox(H2) │ PENDING►SENT►PUBLISHED   │             │
       │                 │       │      │            │  ►FAILED    │             │
       │                 │  vendor│call └────────────┼─────────────┘             │
       │                 │       ▼                   ▼ publish                   │
       │                 │  Mock Vendor         Kafka Producer                   │
       │                 └────────────────────────────┬─────────────────────────┘
       │                                               ▼
       │                                        ┌──────────────┐
       │                                        │    KAFKA     │  sms-events-topic
       │                                        └──────┬───────┘  group: sms-store-group
       │                 ┌────────────────────────────▼─────────────────────────┐
       └──HTTP(history)──┤  GO SMS STORE  :8081   consumer ─► upsert(eventId) ─► MongoDB │
                         └──────────────────────────────────────────────────────┘
```

### Services

| Service | Stack | Role | Port |
|---------|-------|------|------|
| **SMS Sender** | Java 21, Spring Boot 3.2 | Synchronous API. Accepts requests, runs the outbox worker, publishes events. | 8080 |
| **SMS Store** | Go (`net/http`, `segmentio/kafka-go`) | Async worker. Consumes events, persists history, serves queries. | 8081 |
| **Demo Gateway** | Go (stdlib reverse proxy) | Serves the UI, routes API traffic (one origin → no CORS), injects the admin key. | 3000 |

### Infrastructure (Docker)

- **Kafka & Zookeeper** — durable, replayable event bus (`sms-events-topic`).
- **Redis** — in-memory `blocked_users` set; O(1) block check on the hot path.
- **MongoDB** — document store for the SMS event history.
- **H2** *(embedded, not a container)* — the relational, transactional outbox table inside the Java service; file-backed so it survives restarts. Swappable for Postgres via one datasource line.

---

## How It Works

The request path does only a fast local DB write, then returns. A scheduled worker drives each outbox row through a state machine:

```
PENDING ──vendor call (idempotency key = row id)──► SENT ──publish to Kafka──► PUBLISHED
   │                                                  │
   └────────── on repeated failure, with ─────────────┘
               exponential backoff (2s→4s→8s…),
               after max-attempts ──► FAILED (dead-letter, replayable)
```

- **Why two steps?** Once a row is `SENT`, a Kafka failure only retries the *publish* — the vendor is never called again. That's the no-duplicate-SMS guarantee.
- **The idempotency chain:** the outbox row id is both the vendor idempotency key (recipient messaged once) and the `eventId` carried in the Kafka event (MongoDB stores one document, even under redelivery).

---

## Prerequisites

- **Docker** (Docker Desktop or Colima) running
- **Java 21** — the project targets Java 21. *Lombok is incompatible with newer JDKs (e.g. 26); if your default `java` is newer, point `JAVA_HOME` at a JDK 21 install.*
- **Go 1.21+**

---

## Running Locally

Run each in its own terminal.

**1 — Infrastructure (Kafka, Zookeeper, MongoDB, Redis)**
```bash
docker-compose up -d
```
*(Wait ~10–15s for Kafka and Mongo to initialize.)*

**2 — Java Sender**
```bash
cd sms-sender-java
./mvnw spring-boot:run
# If your default JDK is newer than 21:
# JAVA_HOME=/path/to/jdk-21 ./mvnw spring-boot:run
```

**3 — Go Store**
```bash
cd sms-store-go
go run main.go
```

**4 — Demo UI (gateway)**
```bash
cd frontend
go run proxy.go
```
Then open **http://localhost:3000**

> The proxy injects the admin API key on sender-bound traffic. To use a non-default key, set `ADMIN_API_KEY` for **both** the Java service and the proxy.

---

## API Reference

### Java Sender — `http://localhost:8080`

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/v1/sms/send` | public | Accept an SMS request → `202 Accepted` |
| `GET` | `/v1/sms/health` | public | Liveness |
| `GET` | `/v1/sms/block/{phone}` | API key | Check if a number is blocked |
| `POST` | `/v1/sms/block/{phone}` | API key | Block a number |
| `DELETE` | `/v1/sms/block/{phone}` | API key | Unblock a number |
| `GET` | `/v1/sms/outbox` | API key | List recent outbox rows (monitor) |
| `GET` | `/v1/sms/outbox/failed` | API key | List dead-lettered rows |
| `POST` | `/v1/sms/outbox/{id}/replay` | API key | Requeue a `FAILED` row |

#### `POST /v1/sms/send`
```json
// request
{ "phoneNumber": "8888888888", "message": "Hello World" }

// 202 Accepted — durably queued, processed asynchronously
{
  "status": "ACCEPTED",
  "messageId": "c42490c2-ecd1-4bb6-ada2-2b9bfd378fe8",
  "message": "SMS request accepted for processing"
}
```
A blocked number returns `403 Forbidden` (and is never queued). The actual delivery outcome (SUCCESS/FAILED at the vendor) appears later in the message history.

#### Admin auth
Admin endpoints require the `X-API-Key` header (default `demo-secret-key-change-me`, override with the `ADMIN_API_KEY` env var):
```bash
# rejected (no key)
curl -i -X POST localhost:8080/v1/sms/block/7777777777
# accepted
curl -i -X POST localhost:8080/v1/sms/block/7777777777 -H "X-API-Key: demo-secret-key-change-me"
```

### Go Store — `http://localhost:8081`

#### `GET /health`
```json
{ "status": "UP", "timestamp": "2026-06-11T12:00:00Z", "message": "Go SMS Store is running" }
```

#### `GET /v1/user/{phone}/messages`
Returns the stored event history for a number (`userId` maps to `phoneNumber`).
```json
[
  {
    "eventId": "c42490c2-ecd1-4bb6-ada2-2b9bfd378fe8",
    "phoneNumber": "8888888888",
    "message": "Hello World",
    "status": "SUCCESS",
    "vendorMessageId": "VND-68f3573a...",
    "errorReason": "",
    "timestamp": "2026-06-11T12:00:00.000Z"
  }
]
```

---

## The Demo UI

Open **http://localhost:3000** after starting all four terminals.

- **Send SMS** — compose a message; see the `202 Accepted` + request id. Demo numbers: `8888888888` (vendor succeeds), `9999999999` (vendor fails), `7777777777` (use to test blocking).
- **Outbox Monitor** — live table auto-refreshing every 2s; watch rows flow `PENDING → SENT → PUBLISHED`, with status counts and a **Replay** button on `FAILED` rows.
- **Blocklist** — check / block / unblock numbers, with an activity log.
- **History** — fetch the MongoDB-backed history for a number.

### Demoing resilience (optional)
```bash
docker stop kafka     # send a message → it sticks in the outbox, then dead-letters
docker start kafka    # hit "Replay" in the UI → it flows through to PUBLISHED
```
*(For a fast dead-letter, set `outbox.max-attempts: 1` in `application.yml` before the demo, then restore it.)*

---

## Configuration

`sms-sender-java/src/main/resources/application.yml`:

| Setting | Default | Meaning |
|---------|---------|---------|
| `outbox.poll-interval-ms` | `1000` | Worker scan cadence |
| `outbox.max-attempts` | `10` | Failures at one step before dead-lettering |
| `outbox.backoff-base-ms` | `2000` | First retry delay (doubles each attempt) |
| `outbox.backoff-max-ms` | `300000` | Retry-delay cap (5 min) |
| `admin.api-key` | `${ADMIN_API_KEY:demo-secret-key-change-me}` | Shared admin secret |
| `spring.kafka.producer.retries` | `3` | Producer auto-retry on transient failure |

Go store (env vars, with defaults): `SERVER_PORT=8081`, `MONGO_URI=mongodb://localhost:27017`, `MONGO_DB_NAME=sms_db`, `KAFKA_BROKERS=localhost:9092`, `KAFKA_TOPIC=sms-events-topic`.

---

## Project Structure

```
distributed-sms-service/
├── sms-sender-java/        # Java Spring Boot sender
│   └── src/main/java/com/meesho/sms/
│       ├── controller/     # SmsController, OutboxController
│       ├── service/        # block check, outbox write
│       ├── outbox/         # entity, repository, worker, dead-letter/replay
│       ├── vendor/         # idempotent mock vendor
│       ├── event/          # SmsEvent + Kafka publisher
│       ├── security/       # ApiKeyAuthFilter
│       └── config/         # Kafka topic, scheduling
├── sms-store-go/           # Go consumer + history API
│   ├── kafka/              # consumer loop
│   ├── services/           # Mongo upsert (idempotent by eventId)
│   ├── handlers/           # health, history
│   └── models/             # shared SmsEvent
├── frontend/               # single-page UI + reverse-proxy gateway
│   ├── index.html
│   └── proxy.go
├── docs/                   # HLD reference + presentation deck (HTML + PDF)
└── docker-compose.yml      # Kafka, Zookeeper, MongoDB, Redis
```

---

## Documentation

- **`docs/HLD.pdf`** — detailed High-Level Design (architecture, data model, reliability, deployment).
- **`docs/PRESENTATION.pdf`** — 10-slide technical design deck.

---

## Troubleshooting

- **Java fails to start / Connection refused on 6379** — Redis isn't running. Run `docker-compose up -d` and confirm with `docker ps`.
- **Lombok / `cannot find symbol` errors at build** — you're building with a JDK newer than 21. Point `JAVA_HOME` at a JDK 21 install.
- **Outbox rows stuck in `SENT`** — Kafka is unreachable; they'll publish once it's back (or dead-letter after the backoff window).
- **Duplicate history rows** — only happens before the `eventId` upsert; ensure both services are on the latest build, and drop a dirty collection with `docker exec mongodb mongosh sms_db --eval "db.sms_events.drop()"`.
- **Kafka timeouts (`Broker not available`)** — Kafka needs a few seconds to initialize after `docker-compose up -d`.
- **Port conflicts** — ensure `8080`, `8081`, `3000`, `9092`, `2181`, `6379`, `27017` are free.
- **Reset the outbox** — stop the Java service and `rm -rf sms-sender-java/data` (H2 files are runtime state, git-ignored).
