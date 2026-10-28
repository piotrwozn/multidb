package multidb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type TensorDType string

const (
	TensorU8DType   TensorDType = "u8"
	TensorI64DType  TensorDType = "i64"
	TensorF32DType  TensorDType = "f32"
	TensorF64DType  TensorDType = "f64"
	TensorBoolDType TensorDType = "bool"
	TensorUTF8DType TensorDType = "utf8"
)

// TensorU8 encodes bytes as a JSON numeric array, never as a base64 string.
type TensorU8 []uint8

func (values TensorU8) MarshalJSON() ([]byte, error) {
	integers := make([]uint16, len(values))
	for index, value := range values {
		integers[index] = uint16(value)
	}
	return json.Marshal(integers)
}

func (values *TensorU8) UnmarshalJSON(raw []byte) error {
	var integers []uint16
	if err := json.Unmarshal(raw, &integers); err != nil {
		return err
	}
	if integers == nil {
		return fmt.Errorf("tensor u8 data must be a numeric array")
	}
	decoded := make(TensorU8, len(integers))
	for index, value := range integers {
		if value > 255 {
			return fmt.Errorf("tensor u8 value %d exceeds 255", value)
		}
		decoded[index] = uint8(value)
	}
	*values = decoded
	return nil
}

// TensorData is one externally tagged array: set exactly one non-nil field.
// Empty, non-nil arrays represent empty tensors; nil fields are absent.
type TensorData struct {
	U8   TensorU8
	I64  []int64
	F32  []float32
	F64  []float64
	Bool []bool
	UTF8 []string
}

func (data TensorData) MarshalJSON() ([]byte, error) {
	variant := map[string]any{}
	if data.U8 != nil {
		variant["u8"] = data.U8
	}
	if data.I64 != nil {
		variant["i64"] = data.I64
	}
	if data.F32 != nil {
		variant["f32"] = data.F32
	}
	if data.F64 != nil {
		variant["f64"] = data.F64
	}
	if data.Bool != nil {
		variant["bool"] = data.Bool
	}
	if data.UTF8 != nil {
		variant["utf8"] = data.UTF8
	}
	if len(variant) != 1 {
		return nil, fmt.Errorf("tensor data requires exactly one non-nil dtype array")
	}
	return json.Marshal(variant)
}

func (data *TensorData) UnmarshalJSON(raw []byte) error {
	var variant map[string]json.RawMessage
	if err := json.Unmarshal(raw, &variant); err != nil {
		return err
	}
	if len(variant) != 1 {
		return fmt.Errorf("tensor data requires exactly one dtype array")
	}
	decoded := TensorData{}
	for name, values := range variant {
		if bytes.Equal(bytes.TrimSpace(values), []byte("null")) {
			return fmt.Errorf("tensor data must be an array")
		}
		var target any
		switch name {
		case "u8":
			target = &decoded.U8
		case "i64":
			target = &decoded.I64
		case "f32":
			target = &decoded.F32
		case "f64":
			target = &decoded.F64
		case "bool":
			target = &decoded.Bool
		case "utf8":
			target = &decoded.UTF8
		default:
			return fmt.Errorf("unknown tensor dtype %q", name)
		}
		if err := json.Unmarshal(values, target); err != nil {
			return err
		}
	}
	*data = decoded
	return nil
}

type Tensor struct {
	Shape []uint64   `json:"shape"`
	Data  TensorData `json:"data"`
}

// TensorColumn describes one sample. An empty Shape is scalar; nil dimensions
// are ragged. Use an empty non-nil slice for a scalar schema.
type TensorColumn struct {
	Name  string      `json:"name"`
	DType TensorDType `json:"dtype"`
	Shape []*uint64   `json:"shape"`
}

type TensorDatasetConfig struct {
	Name          string         `json:"name"`
	Tensors       []TensorColumn `json:"tensors"`
	ChunkSamples  *int           `json:"chunk_samples,omitempty"`
	MaxChunkBytes *int           `json:"max_chunk_bytes,omitempty"`
}

type TensorBatch map[string][]Tensor

type TensorDatasetInfo struct {
	Config   TensorDatasetConfig `json:"config"`
	Samples  uint64              `json:"samples"`
	Revision uint64              `json:"revision"`
}

type TensorAppendResult struct {
	Start    uint64 `json:"start"`
	End      uint64 `json:"end"`
	Revision uint64 `json:"revision"`
}

type TensorReadRequest struct {
	Start    *uint64   `json:"start,omitempty"`
	Limit    *int      `json:"limit,omitempty"`
	Columns  *[]string `json:"columns,omitempty"`
	Revision *uint64   `json:"revision,omitempty"`
}

type TensorReadBatch struct {
	Start        uint64      `json:"start"`
	End          uint64      `json:"end"`
	TotalSamples uint64      `json:"total_samples"`
	Revision     uint64      `json:"revision"`
	Tensors      TensorBatch `json:"tensors"`
}

type TensorDevice string

const (
	TensorDeviceCPU TensorDevice = "cpu"
	TensorDeviceGPU TensorDevice = "gpu"
)

// TensorExecution reports what the server actually used; the client never
// substitutes CPU work or rewrites a device flag.
type TensorExecution struct {
	Device  TensorDevice `json:"device"`
	Adapter *string      `json:"adapter"`
	Backend *string      `json:"backend"`
}

type TensorMetric string

const (
	TensorMetricCosine TensorMetric = "Cosine"
	TensorMetricL2     TensorMetric = "L2"
)

type TensorSearchRequest struct {
	Tensor     string       `json:"tensor"`
	Query      []float64    `json:"query"`
	K          *int         `json:"k,omitempty"`
	Metric     TensorMetric `json:"metric,omitempty"`
	Start      *uint64      `json:"start,omitempty"`
	MaxSamples *int         `json:"max_samples,omitempty"`
	Revision   *uint64      `json:"revision,omitempty"`
}

type TensorSearchHit struct {
	Index    uint64  `json:"index"`
	Distance float64 `json:"distance"`
}

type TensorSearchResult struct {
	Hits           []TensorSearchHit `json:"hits"`
	ScannedSamples uint64            `json:"scanned_samples"`
	NextStart      *uint64           `json:"next_start"`
	TotalSamples   uint64            `json:"total_samples"`
	Revision       uint64            `json:"revision"`
	SkippedSamples uint64            `json:"skipped_samples"`
	Execution      TensorExecution   `json:"execution"`
}

type TensorGpuOperation string

const (
	TensorGpuSum    TensorGpuOperation = "sum"
	TensorGpuMean   TensorGpuOperation = "mean"
	TensorGpuL2Norm TensorGpuOperation = "l2_norm"
)

type TensorComputeRequest struct {
	Tensor    string             `json:"tensor"`
	Operation TensorGpuOperation `json:"operation"`
	Start     *uint64            `json:"start,omitempty"`
	Limit     *int               `json:"limit,omitempty"`
	Revision  *uint64            `json:"revision,omitempty"`
}

type TensorComputeResult struct {
	Start     uint64          `json:"start"`
	End       uint64          `json:"end"`
	Revision  uint64          `json:"revision"`
	Values    []float32       `json:"values"`
	Execution TensorExecution `json:"execution"`
}

func (client *Client) CreateTensorDataset(ctx context.Context, config TensorDatasetConfig) (TensorDatasetInfo, error) {
	return request[TensorDatasetInfo](client, ctx, http.MethodPost, "/builder/tensor-dataset", config, true)
}

// TensorDatasetInfo reads current metadata when revision is nil; a pointer to 0 requests the
// immutable initial revision rather than silently selecting the current one.
func (client *Client) TensorDatasetInfo(ctx context.Context, dataset string, revision *uint64) (TensorDatasetInfo, error) {
	path := tensorPath(dataset)
	if revision != nil {
		path += "?revision=" + strconv.FormatUint(*revision, 10)
	}
	return request[TensorDatasetInfo](client, ctx, http.MethodGet, path, nil, true)
}

func (client *Client) AppendTensors(ctx context.Context, dataset string, tensors TensorBatch) (TensorAppendResult, error) {
	return request[TensorAppendResult](client, ctx, http.MethodPost, tensorPath(dataset)+"/append", JSONObject{"tensors": tensors}, true)
}

func (client *Client) ReadTensors(ctx context.Context, dataset string, body TensorReadRequest) (TensorReadBatch, error) {
	return request[TensorReadBatch](client, ctx, http.MethodPost, tensorPath(dataset)+"/read", body, true)
}

func (client *Client) SearchTensors(ctx context.Context, dataset string, body TensorSearchRequest) (TensorSearchResult, error) {
	return request[TensorSearchResult](client, ctx, http.MethodPost, tensorPath(dataset)+"/search", body, true)
}

func (client *Client) ComputeTensors(ctx context.Context, dataset string, body TensorComputeRequest) (TensorComputeResult, error) {
	return request[TensorComputeResult](client, ctx, http.MethodPost, tensorPath(dataset)+"/compute", body, true)
}

func tensorPath(dataset string) string { return "/data/tensor-datasets/" + pathEscape(dataset) }
