package services

import (
	"context"
	"log"
	"time"

	"sms-store-go/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SmsService interface {
	SaveEvent(ctx context.Context, event *models.SmsEvent) error
	GetEventsByUser(ctx context.Context, userId string) ([]models.SmsEvent, error)
}

type smsServiceImpl struct {
	collection *mongo.Collection
}

func NewSmsService(db *mongo.Database) SmsService {
	collection := db.Collection("sms_events")
	ensureIndexes(collection)
	return &smsServiceImpl{
		collection: collection,
	}
}

// ensureIndexes creates a sparse, unique index on eventId. This is the safety
// net behind the upsert: even under redelivery or concurrent consumers, the
// same eventId can never produce two documents. Sparse means pre-existing rows
// without an eventId don't break index creation.
func ensureIndexes(collection *mongo.Collection) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	index := mongo.IndexModel{
		Keys:    bson.D{{Key: "eventId", Value: 1}},
		Options: options.Index().SetUnique(true).SetSparse(true).SetName("uniq_eventId"),
	}
	if _, err := collection.Indexes().CreateOne(ctx, index); err != nil {
		log.Printf("Failed to ensure uniq_eventId index: %v\n", err)
	}
}

// SaveEvent is idempotent: it upserts keyed by eventId, so replaying the same
// event from Kafka (at-least-once delivery) always converges to one document.
func (s *smsServiceImpl) SaveEvent(ctx context.Context, event *models.SmsEvent) error {
	filter := bson.M{"eventId": event.EventId}
	opts := options.Replace().SetUpsert(true)
	if _, err := s.collection.ReplaceOne(ctx, filter, event, opts); err != nil {
		log.Printf("Failed to upsert event %s for %s: %v\n", event.EventId, event.PhoneNumber, err)
		return err
	}
	return nil
}

func (s *smsServiceImpl) GetEventsByUser(ctx context.Context, userId string) ([]models.SmsEvent, error) {
	var events []models.SmsEvent

	// We treat userId as phoneNumber based on the Kafka event schema
	filter := bson.M{"phoneNumber": userId}
	cursor, err := s.collection.Find(ctx, filter)
	if err != nil {
		log.Printf("Failed to find events for user %s: %v\n", userId, err)
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &events); err != nil {
		log.Printf("Failed to decode events for user %s: %v\n", userId, err)
		return nil, err
	}

	if events == nil {
		events = []models.SmsEvent{} // Return empty slice instead of nil
	}
	return events, nil
}
