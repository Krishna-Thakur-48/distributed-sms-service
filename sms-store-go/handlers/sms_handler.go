package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"sms-store-go/services"
)

type SmsHandler struct {
	smsService services.SmsService
}

func NewSmsHandler(smsService services.SmsService) *SmsHandler {
	return &SmsHandler{
		smsService: smsService,
	}
}

func (h *SmsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Path parsing: /v1/user/{userId}/messages
	// Expected parts: ["", "v1", "user", "{userId}", "messages"]
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 5 || parts[1] != "v1" || parts[2] != "user" || parts[4] != "messages" {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}

	userId := parts[3]
	if userId == "" {
		http.Error(w, "User ID is required", http.StatusBadRequest)
		return
	}

	events, err := h.smsService.GetEventsByUser(r.Context(), userId)
	if err != nil {
		http.Error(w, "Failed to fetch messages", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(events); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}
