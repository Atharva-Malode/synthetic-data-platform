package server

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testSpecification() DatasetSpecification {
	return DatasetSpecification{Dataset: DatasetConfig{
		Name: "test-dataset", Seed: 42, StartDate: "2026-01-01", EndDate: "2026-01-03",
		UserCount: 2, ActivityMultiplier: 2,
	}, Outputs: OutputConfig{Zip: true}}
}

func TestValidateSpecificationRejectsOversizedJobs(t *testing.T) {
	spec := testSpecification()
	spec.Dataset.UserCount = 600000
	spec.Dataset.ActivityMultiplier = 2
	result := ValidateSpecification(spec)
	if result.Valid || len(result.Errors) == 0 {
		t.Fatalf("expected oversized specification to be rejected: %#v", result)
	}
}

func TestGenerateIsDeterministicAndPreservesRelationships(t *testing.T) {
	spec := testSpecification()
	first := t.TempDir()
	second := t.TempDir()
	if _, err := Generate(context.Background(), spec, first, func(int) {}); err != nil {
		t.Fatal(err)
	}
	if _, err := Generate(context.Background(), spec, second, func(int) {}); err != nil {
		t.Fatal(err)
	}
	firstUsers, err := os.ReadFile(filepath.Join(first, "users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	secondUsers, err := os.ReadFile(filepath.Join(second, "users.csv"))
	if err != nil {
		t.Fatal(err)
	}
	if string(firstUsers) != string(secondUsers) {
		t.Fatal("same seed produced different users.csv output")
	}
	file, err := os.Open(filepath.Join(first, "activity_events.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 || rows[1][1] == "" {
		t.Fatalf("unexpected activity output: %#v", rows)
	}
}

func TestHTTPHealthAndValidationEndpoints(t *testing.T) {
	handler := NewHTTPHandler(NewJobStore())
	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected health response: %d %s", health.Code, health.Body.String())
	}
	body, err := json.Marshal(testSpecification())
	if err != nil {
		t.Fatal(err)
	}
	validation := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/specifications/validate", strings.NewReader(string(body)))
	handler.ServeHTTP(validation, request)
	if validation.Code != http.StatusOK || !strings.Contains(validation.Body.String(), `"valid":true`) {
		t.Fatalf("unexpected validation response: %d %s", validation.Code, validation.Body.String())
	}
}
