# Go SMS Store Service

This is the starter repository for the SMS Store service, built natively in Go using only the standard `net/http` library. This service is designed to eventually consume Kafka events and store them in MongoDB.

## Project Structure

This repository follows a clean, simplified production architecture:

* **`main.go`**: The entry point. It wires up the dependencies, starts the server, and handles graceful shutdowns.
* **`config/`**: Loads and stores environment variables (like ports and future database credentials).
* **`models/`**: Defines the data structures (structs) that represent JSON payloads or database entities.
* **`services/`**: The core business logic layer. Handlers call services to do the actual work.
* **`handlers/`**: The HTTP controllers. They read incoming requests, call the service layer, and write HTTP responses.

## Getting Started

### Prerequisites
* Go 1.21+ installed on your machine.

### Running the Server

Open your terminal and run:
```bash
go run main.go
```
The server will start on port `8081` by default. 

*(Note: We use 8081 so it does not conflict with the Spring Boot server running on 8080).*

### Testing the Endpoints

You can verify the server is running by hitting the health endpoint:

```bash
curl http://localhost:8081/health
```

Expected JSON Response:
```json
{
  "status": "UP",
  "timestamp": "2026-05-19T21:00:00.000Z",
  "message": "Go SMS Store is running"
}
```

## Stopping the Server
Press `Ctrl+C` in the terminal. You will notice the server prints a "Shutting down gracefully..." message. This proves that active connections are given time to finish before the program violently exits.
