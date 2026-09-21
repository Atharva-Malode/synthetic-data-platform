package server

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMCPInitializeAndToolCall(t *testing.T) {
	store := NewJobStore()
	initialize := handleMCPRequest(mcpRequest{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: "initialize"}, store)
	if initialize.Error != nil || initialize.Result == nil {
		t.Fatalf("unexpected initialize response: %#v", initialize)
	}
	spec, err := json.Marshal(testSpecification())
	if err != nil {
		t.Fatal(err)
	}
	result, err := callMCPTool("estimate_generation", spec, store)
	if err != nil || !strings.Contains(result, `"totalRows":6`) {
		t.Fatalf("unexpected estimate result: %s, %v", result, err)
	}
}

func TestMCPRejectsUnknownTool(t *testing.T) {
	_, err := callMCPTool("missing_tool", nil, NewJobStore())
	if err == nil {
		t.Fatal("expected unknown tool error")
	}
}
