package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sms-store-go/config"
	"sms-store-go/handlers"
	"sms-store-go/kafka"
	"sms-store-go/services"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	log.Println("Starting Go SMS Store service...")

	// 1. Load Configuration
	cfg := config.LoadConfig()

	// 2. Initialize Database
	ctx, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelInit()
	
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v\n", err)
	}
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			log.Printf("Failed to disconnect from MongoDB: %v\n", err)
		}
	}()
	db := mongoClient.Database(cfg.MongoDBName)

	// 3. Initialize Services
	healthService := services.NewHealthService()
	smsService := services.NewSmsService(db)

	// 4. Initialize Handlers
	healthHandler := handlers.NewHealthHandler(healthService)
	smsHandler := handlers.NewSmsHandler(smsService)

	// 5. Initialize Kafka Consumer
	kafkaConsumer := kafka.NewConsumer(cfg, smsService)
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	go kafkaConsumer.Start(consumerCtx)

	// 6. Setup Routing
	mux := http.NewServeMux()
	mux.Handle("/health", healthHandler)
	mux.Handle("/v1/user/", smsHandler) // Matches prefix since standard mux

	// 7. Configure HTTP Server
	server := &http.Server{
		Addr:    ":" + cfg.ServerPort,
		Handler: mux,
	}

	// 8. Start Server in a Goroutine (non-blocking)
	go func() {
		log.Printf("Server listening on port %s\n", cfg.ServerPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Listen and serve error: %v\n", err)
		}
	}()

	// 9. Graceful Shutdown Setup
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// Block main thread until a signal is received
	<-stopChan
	log.Println("Received shutdown signal. Shutting down gracefully...")

	// Stop Kafka Consumer
	cancelConsumer()
	if err := kafkaConsumer.Close(); err != nil {
		log.Printf("Error closing Kafka consumer: %v\n", err)
	}

	// Create a context with a timeout so HTTP shutdown doesn't hang forever
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	// Attempt to gracefully shut down the server
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v\n", err)
	}

	log.Println("Server exited properly")
}
