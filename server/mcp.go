package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type mcpRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type mcpResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   interface{} `json:"error,omitempty"`
}

type mcpTool struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

type mcpCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type mcpTextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type mcpCallResult struct {
	Content []mcpTextContent `json:"content"`
	IsError bool             `json:"isError,omitempty"`
}

// RunMCPServer serves newline-delimited JSON-RPC over stdin/stdout.
func RunMCPServer() error {
	store := NewJobStore()
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		var request mcpRequest
		if err := decoder.Decode(&request); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if request.Method == "notifications/initialized" {
			continue
		}
		response := handleMCPRequest(request, store)
		if request.ID == nil {
			continue
		}
		if err := encoder.Encode(response); err != nil {
			return err
		}
	}
}

func handleMCPRequest(request mcpRequest, store *JobStore) mcpResponse {
	response := mcpResponse{JSONRPC: "2.0", ID: json.RawMessage(request.ID)}
	switch request.Method {
	case "initialize":
		response.Result = map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]interface{}{"tools": map[string]interface{}{}},
			"serverInfo":      map[string]string{"name": "synthetic-data-platform", "version": "0.1.0"},
		}
	case "tools/list":
		response.Result = map[string]interface{}{"tools": mcpTools()}
	case "tools/call":
		var params mcpCallParams
		if err := json.Unmarshal(request.Params, &params); err != nil {
			response.Error = mcpError(-32602, "invalid tools/call parameters")
			return response
		}
		result, err := callMCPTool(params.Name, params.Arguments, store)
		if err != nil {
			response.Result = mcpCallResult{Content: []mcpTextContent{{Type: "text", Text: err.Error()}}, IsError: true}
			return response
		}
		response.Result = mcpCallResult{Content: []mcpTextContent{{Type: "text", Text: result}}}
	case "ping":
		response.Result = map[string]interface{}{}
	default:
		response.Error = mcpError(-32601, "method not found")
	}
	return response
}

func mcpTools() []mcpTool {
	specSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"dataset":  map[string]interface{}{"type": "object"},
			"entities": map[string]interface{}{"type": "array"},
			"outputs":  map[string]interface{}{"type": "object"},
		},
		"required": []string{"dataset"},
	}
	return []mcpTool{
		{Name: "validate_generation_spec", Description: "Validate a bounded synthetic data specification.", InputSchema: specSchema},
		{Name: "estimate_generation", Description: "Estimate generated rows and approximate output size.", InputSchema: specSchema},
		{Name: "create_generation_job", Description: "Start a deterministic synthetic data generation job.", InputSchema: specSchema},
		{Name: "get_generation_status", Description: "Get the status and progress of a generation job.", InputSchema: objectSchema("jobId")},
		{Name: "cancel_generation_job", Description: "Cancel a running generation job.", InputSchema: objectSchema("jobId")},
	}
}

func objectSchema(required string) map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			required: map[string]interface{}{"type": "string"},
		},
		"required": []string{required},
	}
}

func callMCPTool(name string, arguments json.RawMessage, store *JobStore) (string, error) {
	switch name {
	case "validate_generation_spec", "estimate_generation", "create_generation_job":
		var spec DatasetSpecification
		if err := json.Unmarshal(arguments, &spec); err != nil {
			return "", fmt.Errorf("invalid specification: %w", err)
		}
		if name == "validate_generation_spec" {
			return marshalMCP(ValidateSpecification(spec))
		}
		validation := ValidateSpecification(spec)
		if !validation.Valid {
			return "", errors.New("specification is invalid: " + validation.Errors[0])
		}
		if name == "estimate_generation" {
			return marshalMCP(EstimateSpecification(spec))
		}
		return createMCPJob(spec, store)
	case "get_generation_status":
		var params struct {
			JobID string `json:"jobId"`
		}
		if err := json.Unmarshal(arguments, &params); err != nil || params.JobID == "" {
			return "", errors.New("jobId is required")
		}
		job, ok := store.Get(params.JobID)
		if !ok {
			return "", errors.New("job not found")
		}
		return marshalMCP(job)
	case "cancel_generation_job":
		var params struct {
			JobID string `json:"jobId"`
		}
		if err := json.Unmarshal(arguments, &params); err != nil || params.JobID == "" {
			return "", errors.New("jobId is required")
		}
		if !store.Cancel(params.JobID) {
			return "", errors.New("job not found or already finished")
		}
		store.Update(params.JobID, func(job *Job) { job.Status = "cancelling" })
		return marshalMCP(map[string]string{"status": "cancelling", "jobId": params.JobID})
	default:
		return "", errors.New("unknown tool: " + name)
	}
}

func createMCPJob(spec DatasetSpecification, store *JobStore) (string, error) {
	id := jobID(spec)
	if existing, ok := store.Get(id); ok {
		return marshalMCP(existing)
	}
	job := &Job{ID: id, Status: "queued", Estimate: EstimateSpecification(spec)}
	store.Put(job)
	ctx, cancel := context.WithCancel(context.Background())
	store.SetCancel(id, cancel)
	go runJob(ctx, store, job, spec)
	return marshalMCP(job)
}

func marshalMCP(value interface{}) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func mcpError(code int, message string) map[string]interface{} {
	return map[string]interface{}{"code": code, "message": message}
}
