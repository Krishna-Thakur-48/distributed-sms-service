# Distributed SMS Service

A polyglot, event-driven microservices architecture for sending, auditing, and storing SMS messages.

## Architecture Overview

The system consists of two primary services communicating asynchronously via Kafka:

1. **SMS Sender (Java / Spring Boot)**
   * **Role:** The synchronous, front-facing REST API.
   * **Behavior:** Receives SMS requests, validates the payload, and checks **Redis** to ensure the user is not blocked. It then attempts to send the message via a Mock SMS Vendor. Regardless of whether the vendor succeeds or fails, an `SmsEvent` is published to Kafka.
   * **Port:** 8080

2. **SMS Store (Go)**
   * **Role:** The asynchronous backend worker and history API.
   * **Behavior:** A native Go `net/http` server that spins up a background Kafka consumer to ingest `sms-events-topic`. It parses the events and stores them in **MongoDB** for historical auditing. It also exposes a REST API to query a user's SMS history.
   * **Port:** 8081

### Infrastructure Components (Docker)
* **Kafka & Zookeeper:** Message broker for the `sms-events-topic`.
* **Redis:** In-memory store used by Java for `blocked_users` sets.
* **MongoDB:** Document database used by Go to store persistent event records.

---

## Local Setup & Getting Started

### Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop) installed and running.
* Java 21+
* Go 1.21+
* (Windows) PowerShell for running the demo script.

### 1. Start Infrastructure
Start the required databases and message broker in the background:
```bash
docker-compose up -d
```
*(Wait about 10-15 seconds for Kafka and MongoDB to fully initialize).*

### 2. Start the Java Sender Service
Open a new terminal and run the Spring Boot application using the provided Maven wrapper:
```bash
cd sms-sender-java
.\mvnw.cmd spring-boot:run
```
*(On Mac/Linux, use `./mvnw spring-boot:run`)*

### 3. Start the Go Store Service
Open another new terminal and start the Go server:
```bash
cd sms-store-go
go run main.go
```

---

## API Endpoints

### Java Service Endpoints (`http://localhost:8080`)

#### `POST /v1/sms/send`
Sends an SMS message.
* **Payload:**
  ```json
  {
    "phoneNumber": "8888888888",
    "message": "Hello World"
  }
  ```
* **Success Response (200 OK):**
  ```json
  {
    "status": "SUCCESS",
    "messageId": "VND-68f3573a-835c-44de-8de9-cc65d92b62e2",
    "message": "SMS sent successfully to 8888888888"
  }
  ```
* **Vendor Failure Response (200 OK):** *(Triggered by sending to `+9999999999`)*
  ```json
  {
    "status": "FAILED",
    "messageId": null,
    "message": "SMS failed to send: Carrier rejected message"
  }
  ```
* **Blocked User Response (403 Forbidden):**
  ```json
  {
    "timestamp": "2026-05-21T12:00:00",
    "status": 403,
    "error": "Forbidden",
    "message": "Phone number is blocked from receiving SMS"
  }
  ```

### Go Service Endpoints (`http://localhost:8081`)

#### `GET /health`
Returns the health status of the Go service.
* **Response (200 OK):**
  ```json
  {
    "status": "UP",
    "timestamp": "2026-05-21T12:00:00Z",
    "message": "Go SMS Store is running"
  }
  ```

#### `GET /v1/user/{userId}/messages`
Retrieves the history of SMS events for a given user. (Note: `userId` maps to the `phoneNumber`).
* **Example:** `GET /v1/user/8888888888/messages`
* **Response (200 OK):**
  ```json
  [
    {
      "phoneNumber": "8888888888",
      "message": "Hello World",
      "status": "SUCCESS",
      "vendorMessageId": "VND-68f3573a...",
      "errorReason": "",
      "timestamp": "2026-05-21T12:00:00.000Z"
    }
  ]
  ```

---

## End-to-End Demo

We have included a PowerShell script (`demo.ps1`) to automatically demonstrate the full system flow. 

**Steps to run manually:**
1. Start infrastructure (`docker-compose up -d`).
2. Add a blocked user to Redis: `docker exec redis redis-cli SADD blocked_users "7777777777"`.
3. Start both the Java and Go services in separate terminals as instructed above.
4. Send an SMS via PowerShell:
   ```powershell
   Invoke-RestMethod -Method POST -Uri "http://localhost:8080/v1/sms/send" -ContentType "application/json" -Body '{"phoneNumber":"8888888888","message":"Test message"}'
   ```
5. Check the Go terminal. You should see a log: `Successfully processed and saved event for 8888888888`.
6. Retrieve the saved message from the Go API:
   ```powershell
   Invoke-RestMethod -Uri "http://localhost:8081/v1/user/8888888888/messages"
   ```

*(You can run `.\demo.ps1` in PowerShell to execute these API calls sequentially).*

---

## Troubleshooting

* **Java service fails to start / Connection Refused on 6379:** Your Redis container isn't running. Ensure you ran `docker-compose up -d` and that `docker ps` shows the `redis` container.
* **Go service isn't receiving messages:** Make sure the Java service successfully compiled with the `OffsetDateTime` fixes. The Go service listens on `sms-events-topic`.
* **Kafka timeouts (`Broker not available`):** Kafka takes a few seconds to fully initialize inside Docker. Give it a moment after running `docker-compose up -d` before firing events.
* **Port conflicts:** Ensure nothing else on your machine is using ports `8080` (Java), `8081` (Go), `9092` (Kafka), `2181` (ZK), `6379` (Redis), or `27017` (Mongo).
