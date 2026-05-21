package services

import (
	"sms-store-go/models"
	"time"
)

type HealthService struct{}

func NewHealthService() *HealthService {
	return &HealthService{}
}

func (s *HealthService) CheckHealth() models.HealthResponse {
	return models.HealthResponse{
		Status:    "UP",
		Timestamp: time.Now(),
		Message:   "Go SMS Store is running",
	}
}
