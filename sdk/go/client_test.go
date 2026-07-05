package multidb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCompatibilityConstants(t *testing.T) {
	if DefaultBaseURL != "http://127.0.0.1:8080/api" {
		t.Fatalf("unexpected default base URL %q", DefaultBaseURL)
	}
	if ControlPlaneAPIVersion != 1 {
		t.Fatalf("unexpected API version %d", ControlPlaneAPIVersion)
	}
	if MinMultiDBVersion != "0.1.0" {
		t.Fatalf("unexpected minimum MultiDB version %q", MinMultiDBVersion)
	}
}

func TestClientMapsSuccessAndErrorEnvelopes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("missing bearer header: %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("content-type", "application/json")
		if r.URL.Path == "/api/status" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "data": map[string]any{"server_version": "test"}})
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": map[string]any{"code": "unauthorized", "message": "unauthorized"}})
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL+"/api"), WithToken("secret"))
	status, err := client.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status["server_version"] != "test" {
		t.Fatalf("unexpected status: %#v", status)
	}
	_, err = client.AuthMe(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T", err)
	}
	if apiErr.Status != http.StatusUnauthorized || apiErr.Code != "unauthorized" {
		t.Fatalf("unexpected APIError: %#v", apiErr)
	}
}

func TestClientMapsInvalidEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "not-envelope"})
	}))
	defer server.Close()

	_, err := NewClient(WithBaseURL(server.URL)).Status(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T", err)
	}
	if apiErr.Code != "invalid_envelope" {
		t.Fatalf("unexpected code %q", apiErr.Code)
	}
}

func TestRawHealthIsNotEnveloped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(HealthResponse{OK: true, Status: "alive"})
	}))
	defer server.Close()

	health, err := NewClient(WithBaseURL(server.URL)).Health(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !health.OK || health.Status != "alive" {
		t.Fatalf("unexpected health: %#v", health)
	}
}
