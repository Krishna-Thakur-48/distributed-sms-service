package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sms-store-go/models"
)

type mockSmsService struct {
	events []models.SmsEvent
	err    error
}

func (m *mockSmsService) SaveEvent(ctx context.Context, event *models.SmsEvent) error {
	m.events = append(m.events, *event)
	return m.err
}

func (m *mockSmsService) GetEventsByUser(ctx context.Context, userId string) ([]models.SmsEvent, error) {
	if m.err != nil {
		return nil, m.err
	}
	var res []models.SmsEvent
	for _, e := range m.events {
		if e.PhoneNumber == userId {
			res = append(res, e)
		}
	}
	return res, nil
}

func TestSmsHandler_ServeHTTP(t *testing.T) {
	mockSvc := &mockSmsService{
		events: []models.SmsEvent{
			{PhoneNumber: "1234567890", Message: "Hello", Status: "SUCCESS", Timestamp: time.Now()},
		},
	}
	handler := NewSmsHandler(mockSvc)

	t.Run("Valid GET request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/user/1234567890/messages", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		var resp []models.SmsEvent
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		if len(resp) != 1 || resp[0].Message != "Hello" {
			t.Errorf("Unexpected response content: %+v", resp)
		}
	})

	t.Run("Invalid method", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/user/1234567890/messages", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
		}
	})

	t.Run("Invalid path format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/user/1234567890", nil) // missing /messages
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status %d, got %d", http.StatusNotFound, w.Code)
		}
	})
}
