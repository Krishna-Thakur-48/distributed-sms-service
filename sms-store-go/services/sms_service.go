package services

import (
	"context"
	"log"

	"sms-store-go/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type SmsService interface {
	SaveEvent(ctx context.Context, event *models.SmsEvent) error
	GetEventsByUser(ctx context.Context, userId string) ([]models.SmsEvent, error)
}

type smsServiceImpl struct {
	collection *mongo.Collection
}

func NewSmsService(db *mongo.Database) SmsService {
	return &smsServiceImpl{
		collection: db.Collection("sms_events"),
	}
}

func (s *smsServiceImpl) SaveEvent(ctx context.Context, event *models.SmsEvent) error {
	_, err := s.collection.InsertOne(ctx, event)
	if err != nil {
		log.Printf("Failed to insert event for %s: %v\n", event.PhoneNumber, err)
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
