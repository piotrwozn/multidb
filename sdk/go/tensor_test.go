package multidb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestTensorDataWireFormatPreservesAllSixDTypesAndNumericBytes(t *testing.T) {
	for _, data := range []TensorData{
		{U8: TensorU8{0, 128, 255}}, {I64: []int64{-9223372036854775807, 9223372036854775807}},
		{F32: []float32{0.5}}, {F64: []float64{1.25}}, {Bool: []bool{false, true}},
		{UTF8: []string{"hello", "żółć"}}, {U8: TensorU8{}}, {F32: []float32{}},
	} {
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		if data.U8 != nil && strings.Contains(string(raw), `"u8":"`) {
			t.Fatalf("u8 became base64: %s", raw)
		}
		var decoded TensorData
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decoded, data) {
			t.Fatalf("dtype round trip lost data: %s / %#v", raw, decoded)
		}
	}
	for _, invalid := range []string{`{}`, `{"f32":[],"u8":[]}`, `{"u8":"AA=="}`, `{"u8":[256]}`, `{"u8":[-1]}`, `{"f32":null}`, `{"unknown":[]}`} {
		var data TensorData
		if json.Unmarshal([]byte(invalid), &data) == nil {
			t.Fatalf("accepted invalid tensor data %s", invalid)
		}
	}
	if _, err := json.Marshal(TensorData{U8: TensorU8{}, F32: []float32{}}); err == nil {
		t.Fatal("serialized two dtype variants")
	}
}

func TestTensorMethodsUseTypedPayloadsRevisionZeroAuthAndEscapedNames(t *testing.T) {
	type call struct {
		method, uri string
		body        JSONObject
	}
	calls := make(chan call, 7)
	dimension := uint64(2)
	config := TensorDatasetConfig{Name: "training", Tensors: []TensorColumn{
		{Name: "embedding", DType: TensorF32DType, Shape: []*uint64{&dimension}},
		{Name: "image", DType: TensorU8DType, Shape: []*uint64{nil}},
		{Name: "text", DType: TensorUTF8DType, Shape: []*uint64{}},
	}}
	batch := TensorBatch{
		"embedding": {{Shape: []uint64{2}, Data: TensorData{F32: []float32{1, 0}}}},
		"image":     {{Shape: []uint64{3}, Data: TensorData{U8: TensorU8{0, 128, 255}}}},
		"text":      {{Shape: []uint64{}, Data: TensorData{UTF8: []string{"caption"}}}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("x-multidb-principal") != "alice" {
			t.Error("tensor request lost auth headers")
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Error("tensor request lost JSON accept header")
		}
		var body JSONObject
		if r.Method != http.MethodGet {
			if r.Header.Get("Content-Type") != "application/json" {
				t.Error("tensor request lost JSON content type")
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
		}
		calls <- call{r.Method, r.URL.RequestURI(), body}
		var data any = JSONObject{"config": config, "samples": 0, "revision": 0}
		switch {
		case strings.HasSuffix(r.URL.Path, "/append"):
			data = JSONObject{"start": 0, "end": 1, "revision": 1}
		case strings.HasSuffix(r.URL.Path, "/read"):
			data = JSONObject{"start": 0, "end": 1, "total_samples": 1, "revision": 1, "tensors": batch}
		case strings.HasSuffix(r.URL.Path, "/search"):
			data = JSONObject{"hits": []JSONObject{{"index": 0, "distance": 0.25}}, "scanned_samples": 1, "next_start": 0, "total_samples": 2, "revision": 1, "skipped_samples": 0,
				"execution": JSONObject{"device": "gpu", "adapter": "hardware", "backend": "Vulkan"}}
		case strings.HasSuffix(r.URL.Path, "/compute"):
			data = JSONObject{"start": 0, "end": 1, "revision": 1, "values": []float32{1}, "execution": JSONObject{"device": "gpu", "adapter": "hardware", "backend": "Vulkan"}}
		}
		_ = json.NewEncoder(w).Encode(JSONObject{"ok": true, "data": data})
	}))
	defer server.Close()
	client := NewClient(WithBaseURL(server.URL+"/api"), WithToken("secret"), WithPrincipal("alice"))
	ctx := context.Background()
	if _, err := client.CreateTensorDataset(ctx, config); err != nil {
		t.Fatal(err)
	}
	name := "training /?#%ą"
	zero := uint64(0)
	if _, err := client.TensorDatasetInfo(ctx, name, nil); err != nil {
		t.Fatal(err)
	}
	if info, err := client.TensorDatasetInfo(ctx, name, &zero); err != nil || info.Revision != 0 {
		t.Fatalf("initial revision failed: %#v %v", info, err)
	}
	if written, err := client.AppendTensors(ctx, name, batch); err != nil || written.Revision != 1 {
		t.Fatalf("append: %#v %v", written, err)
	}
	limit := 2
	columns := []string{"embedding"}
	read, err := client.ReadTensors(ctx, name, TensorReadRequest{Start: &zero, Limit: &limit, Revision: &zero, Columns: &columns})
	if err != nil || !reflect.DeepEqual(read.Tensors, batch) {
		t.Fatalf("read lost typed tensors: %#v %v", read, err)
	}
	search, err := client.SearchTensors(ctx, name, TensorSearchRequest{Tensor: "embedding", Query: []float64{1, 0}, Metric: TensorMetricL2, Start: &zero, Revision: &zero})
	if err != nil || search.Execution.Device != TensorDeviceGPU || search.NextStart == nil || *search.NextStart != 0 {
		t.Fatalf("search: %#v %v", search, err)
	}
	if search.Execution.Adapter == nil || *search.Execution.Adapter != "hardware" || search.Execution.Backend == nil || *search.Execution.Backend != "Vulkan" {
		t.Fatalf("execution attribution lost: %#v", search.Execution)
	}
	computed, err := client.ComputeTensors(ctx, name, TensorComputeRequest{Tensor: "embedding", Operation: TensorGpuL2Norm, Start: &zero, Limit: &limit, Revision: &zero})
	if err != nil || computed.Execution.Device != TensorDeviceGPU || !reflect.DeepEqual(computed.Values, []float32{1}) {
		t.Fatalf("compute: %#v %v", computed, err)
	}
	base := "/api/data/tensor-datasets/training%20%2F%3F%23%25%C4%85"
	for index, want := range []string{"/api/builder/tensor-dataset", base, base + "?revision=0", base + "/append", base + "/read", base + "/search", base + "/compute"} {
		got := <-calls
		method := http.MethodPost
		if index == 1 || index == 2 {
			method = http.MethodGet
		}
		if got.method != method || got.uri != want {
			t.Fatalf("call%d: %#v, want%s %s", index, got, method, want)
		}
		if index == 0 {
			if _, exists := got.body["chunk_samples"]; exists {
				t.Fatal("omitted chunk default was sent")
			}
		}
		if index >= 4 && (got.body["start"] != float64(0) || got.body["revision"] != float64(0)) {
			t.Fatalf("zero options omitted: %#v", got.body)
		}
		if index == 3 {
			image := got.body["tensors"].(map[string]any)["image"].([]any)[0].(map[string]any)["data"].(map[string]any)["u8"]
			if !reflect.DeepEqual(image, []any{float64(0), float64(128), float64(255)}) {
				t.Fatalf("append did not use numeric u8: %#v", image)
			}
		}
	}
}

func TestTensorGPUUnavailableIsReturnedWithoutFallbackOrRetriedRequests(t *testing.T) {
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Path
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(JSONObject{"ok": false, "error": JSONObject{"code": "tensor_gpu_unavailable", "message": "native GPU required"}})
	}))
	defer server.Close()
	client := NewClient(WithBaseURL(server.URL))
	_, searchErr := client.SearchTensors(context.Background(), "training", TensorSearchRequest{Tensor: "embedding", Query: []float64{1}})
	_, computeErr := client.ComputeTensors(context.Background(), "training", TensorComputeRequest{Tensor: "embedding", Operation: TensorGpuSum})
	for _, err := range []error{searchErr, computeErr} {
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Status != 503 || apiErr.Code != "tensor_gpu_unavailable" || apiErr.Message != "native GPU required" {
			t.Fatalf("GPU error lost: %#v", err)
		}
	}
	if len(requests) != 2 {
		t.Fatalf("client retried unavailable GPU: %d calls", len(requests))
	}
}
