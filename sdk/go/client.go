package multidb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	DefaultBaseURL          = "http://127.0.0.1:8080/api"
	ControlPlaneAPIVersion = 1
	MinMultiDBVersion      = "0.1.0"
)

type JSONValue = any
type JSONObject = map[string]any

type Client struct {
	baseURL    string
	token      string
	principal  string
	httpClient *http.Client
}

type Option func(*Client)

func WithBaseURL(baseURL string) Option {
	return func(client *Client) {
		client.baseURL = strings.TrimRight(baseURL, "/")
	}
}

func WithToken(token string) Option {
	return func(client *Client) {
		client.token = strings.TrimSpace(token)
	}
}

func WithPrincipal(principal string) Option {
	return func(client *Client) {
		client.principal = strings.TrimSpace(principal)
	}
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(client *Client) {
		if httpClient != nil {
			client.httpClient = httpClient
		}
	}
}

func NewClient(options ...Option) *Client {
	client := &Client{baseURL: DefaultBaseURL, httpClient: http.DefaultClient}
	for _, option := range options {
		option(client)
	}
	return client
}

func (client *Client) WithToken(token string) *Client {
	return NewClient(
		WithBaseURL(client.baseURL),
		WithToken(token),
		WithPrincipal(client.principal),
		WithHTTPClient(client.httpClient),
	)
}

type APIError struct {
	Status  int
	Code    string
	Message string
	Body    []byte
}

func (err *APIError) Error() string {
	return fmt.Sprintf("multidb control plane %s (%d): %s", err.Code, err.Status, err.Message)
}

type LoginResponse struct {
	Token           string   `json:"token"`
	ExpiresAt       string   `json:"expires_at"`
	ExpiresAtMillis uint64   `json:"expires_at_millis"`
	Principal       string   `json:"principal"`
	Roles           []string `json:"roles"`
}

type AuthMeResponse struct {
	Principal          string   `json:"principal"`
	Roles              []string `json:"roles"`
	SystemAdmin        bool     `json:"system_admin"`
	DatabaseAdmin      bool     `json:"database_admin"`
	InsecureLocalAdmin bool     `json:"insecure_local_admin"`
}

type HealthResponse struct {
	OK     bool   `json:"ok"`
	Status string `json:"status"`
}

type AdminStatus map[string]any
type StudioManifest map[string]any

func (client *Client) OpenAPI(ctx context.Context) (JSONObject, error) {
	return rawJSON[JSONObject](client, ctx, http.MethodGet, "/openapi.json", nil, false)
}

func (client *Client) Health(ctx context.Context) (HealthResponse, error) {
	return rawJSON[HealthResponse](client, ctx, http.MethodGet, "/health", nil, false)
}

func (client *Client) Ready(ctx context.Context) (HealthResponse, error) {
	return rawJSON[HealthResponse](client, ctx, http.MethodGet, "/ready", nil, false)
}

func (client *Client) Status(ctx context.Context) (AdminStatus, error) {
	return request[AdminStatus](client, ctx, http.MethodGet, "/status", nil, true)
}

func (client *Client) Metrics(ctx context.Context) (string, error) {
	status, body, err := client.send(ctx, http.MethodGet, "/metrics", nil, true)
	if err != nil {
		return "", err
	}
	if status >= 400 {
		return "", httpError(status, body)
	}
	return string(body), nil
}

func (client *Client) Login(ctx context.Context, username, password string) (LoginResponse, error) {
	return request[LoginResponse](client, ctx, http.MethodPost, "/auth/login", JSONObject{"username": username, "password": password}, false)
}

func (client *Client) Logout(ctx context.Context) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/auth/logout", nil, true)
}

func (client *Client) ChangePassword(ctx context.Context, currentPassword, newPassword string) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/auth/change-password", JSONObject{"current_password": currentPassword, "new_password": newPassword}, true)
}

func (client *Client) AuthMe(ctx context.Context) (AuthMeResponse, error) {
	return request[AuthMeResponse](client, ctx, http.MethodGet, "/auth/me", nil, true)
}

func (client *Client) Catalog(ctx context.Context) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodGet, "/catalog", nil, true)
}

func (client *Client) SQL(ctx context.Context, statement string) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/sql", JSONObject{"sql": statement}, true)
}

func (client *Client) TableRows(ctx context.Context, table string, offset, limit int) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodGet, fmt.Sprintf("/data/tables/%s/rows?offset=%d&limit=%d", pathEscape(table), offset, limit), nil, true)
}

func (client *Client) InsertTableRow(ctx context.Context, table string, row []JSONValue) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/data/tables/"+pathEscape(table)+"/rows", JSONObject{"row": row}, true)
}

func (client *Client) UpdateTableRow(ctx context.Context, table string, row []JSONValue) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPut, "/data/tables/"+pathEscape(table)+"/rows", JSONObject{"row": row}, true)
}

func (client *Client) DeleteTableRow(ctx context.Context, table string, primaryKey JSONValue, confirm string) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodDelete, "/data/tables/"+pathEscape(table)+"/rows", JSONObject{"primary_key": primaryKey, "confirm": confirm}, true)
}

func (client *Client) Documents(ctx context.Context, collection string, offset, limit int) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodGet, fmt.Sprintf("/data/collections/%s/documents?offset=%d&limit=%d", pathEscape(collection), offset, limit), nil, true)
}

func (client *Client) CreateDocument(ctx context.Context, collection string, document JSONValue) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/data/collections/"+pathEscape(collection)+"/documents", JSONObject{"document": document}, true)
}

func (client *Client) UpdateDocument(ctx context.Context, collection, id string, document JSONValue) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPut, "/data/collections/"+pathEscape(collection)+"/documents/"+pathEscape(id), JSONObject{"document": document}, true)
}

func (client *Client) DeleteDocument(ctx context.Context, collection, id, confirm string) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodDelete, "/data/collections/"+pathEscape(collection)+"/documents/"+pathEscape(id), JSONObject{"confirm": confirm}, true)
}

func (client *Client) CreateTable(ctx context.Context, body JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/builder/table", body, true)
}

func (client *Client) CreateCollection(ctx context.Context, body JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/builder/collection", body, true)
}

func (client *Client) CreateVector(ctx context.Context, body JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/builder/vector", body, true)
}

func (client *Client) CreateTimeSeries(ctx context.Context, body JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/builder/time-series", body, true)
}

func (client *Client) CreateFullText(ctx context.Context, body JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/builder/full-text", body, true)
}

func (client *Client) CreateGeoIndex(ctx context.Context, body JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/builder/geo", body, true)
}

func (client *Client) CreateGraph(ctx context.Context, body JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/builder/graph", body, true)
}

func (client *Client) InsertVector(ctx context.Context, collection string, metadata JSONValue, vector []float64) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/data/vectors/"+pathEscape(collection)+"/vectors", JSONObject{"metadata": metadata, "vector": vector}, true)
}

func (client *Client) SearchVector(ctx context.Context, collection string, vector []float64, k int) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/data/vectors/"+pathEscape(collection)+"/search", JSONObject{"vector": vector, "k": k}, true)
}

func (client *Client) TimeSeriesPoints(ctx context.Context, collection, series string, start, end int64) (JSONObject, error) {
	path := fmt.Sprintf("/data/time-series/%s/points?series=%s&start=%d&end=%d", pathEscape(collection), url.QueryEscape(series), start, end)
	return request[JSONObject](client, ctx, http.MethodGet, path, nil, true)
}

func (client *Client) InsertTimeSeriesPoint(ctx context.Context, collection, series string, point JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/data/time-series/"+pathEscape(collection)+"/points", JSONObject{"series": series, "point": point}, true)
}

func (client *Client) Security(ctx context.Context) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodGet, "/security", nil, true)
}

func (client *Client) SaveSecurity(ctx context.Context, security JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/security", security, true)
}

func (client *Client) Audit(ctx context.Context) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodGet, "/audit", nil, true)
}

func (client *Client) Config(ctx context.Context) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodGet, "/config", nil, true)
}

func (client *Client) Validate(ctx context.Context, spec JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/config/validate", spec, true)
}

func (client *Client) Plan(ctx context.Context, current, desired JSONObject) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/config/plan", JSONObject{"current": current, "desired": desired}, true)
}

func (client *Client) Apply(ctx context.Context, plan JSONObject, confirm string) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/config/apply", JSONObject{"plan": plan, "confirm": confirm}, true)
}

func (client *Client) Profiles(ctx context.Context) ([]any, error) {
	return request[[]any](client, ctx, http.MethodGet, "/profiles", nil, true)
}

func (client *Client) Roles(ctx context.Context) ([]any, error) {
	return request[[]any](client, ctx, http.MethodGet, "/roles", nil, true)
}

func (client *Client) Domains(ctx context.Context) ([]any, error) {
	return request[[]any](client, ctx, http.MethodGet, "/domains", nil, true)
}

func (client *Client) Extensions(ctx context.Context) ([]any, error) {
	return request[[]any](client, ctx, http.MethodGet, "/extensions", nil, true)
}

func (client *Client) Advice(ctx context.Context) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodGet, "/advice", nil, true)
}

func (client *Client) AdvicePlan(ctx context.Context, adviceID string) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/advice/plan", JSONObject{"advice_id": adviceID}, true)
}

func (client *Client) RecordAdviceDecision(ctx context.Context, adviceID, status, reason string) (JSONObject, error) {
	return request[JSONObject](client, ctx, http.MethodPost, "/advice/decision", JSONObject{"advice_id": adviceID, "status": status, "reason": reason}, true)
}

func (client *Client) StudioManifest(ctx context.Context) (StudioManifest, error) {
	return request[StudioManifest](client, ctx, http.MethodGet, "/studio", nil, true)
}

func request[T any](client *Client, ctx context.Context, method, path string, body any, auth bool) (T, error) {
	var zero T
	status, raw, err := client.send(ctx, method, path, body, auth)
	if err != nil {
		return zero, err
	}
	var envelope struct {
		OK    *bool           `json:"ok"`
		Data  json.RawMessage `json:"data"`
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return zero, &APIError{Status: status, Code: "invalid_json", Message: "Control Plane did not return JSON", Body: raw}
	}
	if envelope.OK == nil {
		return zero, &APIError{Status: status, Code: "invalid_envelope", Message: "Control Plane returned an invalid envelope", Body: raw}
	}
	if !*envelope.OK {
		return zero, &APIError{Status: status, Code: envelope.Error.Code, Message: envelope.Error.Message, Body: raw}
	}
	if len(envelope.Data) == 0 {
		return zero, nil
	}
	var data T
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		return zero, &APIError{Status: status, Code: "invalid_json", Message: err.Error(), Body: envelope.Data}
	}
	return data, nil
}

func rawJSON[T any](client *Client, ctx context.Context, method, path string, body any, auth bool) (T, error) {
	var zero T
	status, raw, err := client.send(ctx, method, path, body, auth)
	if err != nil {
		return zero, err
	}
	var data T
	if err := json.Unmarshal(raw, &data); err != nil {
		return zero, &APIError{Status: status, Code: "invalid_json", Message: "Control Plane did not return JSON", Body: raw}
	}
	return data, nil
}

func (client *Client) send(ctx context.Context, method, path string, body any, auth bool) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, client.baseURL+path, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+client.token)
		if client.principal != "" {
			req.Header.Set("x-multidb-principal", client.principal)
		}
	}
	resp, err := client.httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if resp.StatusCode >= 400 && len(raw) == 0 {
		return resp.StatusCode, raw, httpError(resp.StatusCode, raw)
	}
	return resp.StatusCode, raw, nil
}

func httpError(status int, body []byte) error {
	return &APIError{Status: status, Code: fmt.Sprintf("http_%d", status), Message: string(body), Body: body}
}

func pathEscape(value string) string {
	return url.PathEscape(value)
}
