package models

import "time"

type SmsEvent struct {
	EventId         string    `json:"eventId" bson:"eventId"`
	PhoneNumber     string    `json:"phoneNumber" bson:"phoneNumber"`
	Message         string    `json:"message" bson:"message"`
	Status          string    `json:"status" bson:"status"`
	VendorMessageId string    `json:"vendorMessageId" bson:"vendorMessageId"`
	ErrorReason     string    `json:"errorReason" bson:"errorReason"`
	Timestamp       time.Time `json:"timestamp" bson:"timestamp"`
}
