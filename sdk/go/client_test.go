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

func TestProvisioningMethodsSendDocumentedPathsAndBodies(t *testing.T) {
	type call struct {
		method string
		path   string
		body   map[string]any
	}
	var calls []call
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		calls = append(calls, call{method: r.Method, path: r.URL.Path, body: body})
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "data": map[string]any{}})
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL+"/api"), WithToken("secret"))
	ctx := context.Background()
	spec := JSONObject{"version": float64(2), "name": "shop"}
	options := JSONObject{"allow_destructive": []any{"collections.orders"}}
	if _, err := client.ProvisionState(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ProvisionPlan(ctx, spec, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ProvisionApply(ctx, spec, "prov_0123", options); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ProvisionDrift(ctx, spec, options); err != nil {
		t.Fatal(err)
	}
	if _, err := client.LiveConfig(ctx); err != nil {
		t.Fatal(err)
	}

	want := []call{
		{method: http.MethodGet, path: "/api/provision/state"},
		{method: http.MethodPost, path: "/api/provision/plan", body: map[string]any{"spec": map[string]any(spec)}},
		{method: http.MethodPost, path: "/api/provision/apply", body: map[string]any{"spec": map[string]any(spec), "options": map[string]any(options), "confirm": "prov_0123"}},
		{method: http.MethodPost, path: "/api/provision/drift", body: map[string]any{"spec": map[string]any(spec), "options": map[string]any(options)}},
		{method: http.MethodGet, path: "/api/config/live"},
	}
	if len(calls) != len(want) {
		t.Fatalf("unexpected calls: %#v", calls)
	}
	for index, expected := range want {
		got := calls[index]
		if got.method != expected.method || got.path != expected.path {
			t.Fatalf("call %d: got %s %s, want %s %s", index, got.method, got.path, expected.method, expected.path)
		}
		gotBody, _ := json.Marshal(got.body)
		wantBody, _ := json.Marshal(expected.body)
		if string(gotBody) != string(wantBody) {
			t.Fatalf("call %d body: got %s, want %s", index, gotBody, wantBody)
		}
	}
}

func TestBlockedProvisionPlanKeepsThePlanInTheError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":    false,
			"error": map[string]any{"code": "provision_blocked", "message": "blocked"},
			"data":  map[string]any{"plan_id": "prov_0123", "outcome": "blocked"},
		})
	}))
	defer server.Close()

	_, err := NewClient(WithBaseURL(server.URL)).ProvisionPlan(context.Background(), JSONObject{"version": 2}, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T", err)
	}
	if apiErr.Status != http.StatusUnprocessableEntity || apiErr.Code != "provision_blocked" {
		t.Fatalf("unexpected APIError: %#v", apiErr)
	}
	var body map[string]any
	if err := json.Unmarshal(apiErr.Body, &body); err != nil {
		t.Fatal(err)
	}
	if plan, ok := body["data"].(map[string]any); !ok || plan["plan_id"] != "prov_0123" {
		t.Fatalf("the error body does not carry the plan: %s", apiErr.Body)
	}
}
