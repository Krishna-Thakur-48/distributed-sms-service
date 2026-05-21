package kafka

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"sms-store-go/config"
	"sms-store-go/models"
	"sms-store-go/services"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader     *kafka.Reader
	smsService services.SmsService
}

func NewConsumer(cfg *config.Config, smsService services.SmsService) *Consumer {
	brokers := strings.Split(cfg.KafkaBrokers, ",")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    cfg.KafkaTopic,
		GroupID:  "sms-store-group",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	return &Consumer{
		reader:     r,
		smsService: smsService,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	log.Println("Starting Kafka consumer...")
	for {
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Kafka consumer stopped by context")
				return
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		log.Printf("Received message from topic %s: %s", m.Topic, string(m.Value))

		var event models.SmsEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Failed to unmarshal event JSON: %v. Raw value: %s", err, string(m.Value))
			continue
		}

		// Save to MongoDB
		if err := c.smsService.SaveEvent(ctx, &event); err != nil {
			log.Printf("Failed to save event to DB: %v", err)
		} else {
			log.Printf("Successfully processed and saved event for %s", event.PhoneNumber)
		}
	}
}

func (c *Consumer) Close() error {
	log.Println("Closing Kafka consumer...")
	return c.reader.Close()
}
