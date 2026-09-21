package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/dicedb/dice/config"
)

func RunHTTPServer() error {
	store := NewJobStore()
	server := &http.Server{Addr: fmt.Sprintf("%s:%d", config.Host, config.HTTPPort), Handler: NewHTTPHandler(store)}
	return server.ListenAndServe()
}

func NewHTTPHandler(store *JobStore) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", healthHandler)
	mux.HandleFunc("/api/specifications/validate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		var spec DatasetSpecification
		if err := decodeJSON(r, &spec); err != nil {
			writeErrorResponse(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, ValidateSpecification(spec))
	})
	mux.HandleFunc("/api/jobs/estimate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		var spec DatasetSpecification
		if err := decodeJSON(r, &spec); err != nil {
			writeErrorResponse(w, http.StatusBadRequest, err)
			return
		}
		validation := ValidateSpecification(spec)
		if !validation.Valid {
			writeJSON(w, http.StatusUnprocessableEntity, validation)
			return
		}
		writeJSON(w, http.StatusOK, EstimateSpecification(spec))
	})
	mux.HandleFunc("/api/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		createJob(w, r, store)
	})
	mux.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
		handleJobRoute(w, r, store)
	})

	return mux
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func createJob(w http.ResponseWriter, r *http.Request, store *JobStore) {
	var spec DatasetSpecification
	if err := decodeJSON(r, &spec); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, err)
		return
	}
	validation := ValidateSpecification(spec)
	if !validation.Valid {
		writeJSON(w, http.StatusUnprocessableEntity, validation)
		return
	}
	id := jobID(spec)
	if existing, ok := store.Get(id); ok {
		writeJSON(w, http.StatusOK, existing)
		return
	}
	job := &Job{ID: id, Status: "queued", Estimate: EstimateSpecification(spec), CreatedAt: time.Now().UTC()}
	store.Put(job)
	ctx, cancel := context.WithCancel(context.Background())
	store.SetCancel(id, cancel)
	go runJob(ctx, store, job, spec)
	writeJSON(w, http.StatusAccepted, job)
}

func runJob(ctx context.Context, store *JobStore, job *Job, spec DatasetSpecification) {
	store.Update(job.ID, func(job *Job) { job.Status = "running" })
	outputDir := filepath.Join("data", job.ID)
	paths, err := Generate(ctx, spec, outputDir, func(progress int) {
		store.Update(job.ID, func(job *Job) { job.Progress = progress })
	})
	if errors.Is(err, context.Canceled) {
		store.Update(job.ID, func(job *Job) { job.Status = "cancelled"; job.Error = "job cancelled" })
		return
	}
	if err != nil {
		store.Update(job.ID, func(job *Job) { job.Status = "failed"; job.Error = err.Error() })
		return
	}
	store.Update(job.ID, func(job *Job) { job.Status = "completed"; job.Progress = 100; job.Exports = paths })
}

func handleJobRoute(w http.ResponseWriter, r *http.Request, store *JobStore) {
	parts := strings.Split(strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, "/"), "/api/jobs/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErrorResponse(w, http.StatusNotFound, errors.New("job not found"))
		return
	}
	id := parts[0]
	job, ok := store.Get(id)
	if !ok {
		writeErrorResponse(w, http.StatusNotFound, errors.New("job not found"))
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, job)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
		if job.Status == "completed" || job.Status == "failed" || job.Status == "cancelled" {
			writeJSON(w, http.StatusConflict, job)
			return
		}
		store.Cancel(id)
		store.Update(id, func(job *Job) { job.Status = "cancelling" })
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "cancelling"})
		return
	}
	if len(parts) == 2 && parts[1] == "exports" && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string][]string{"exports": job.Exports})
		return
	}
	methodNotAllowed(w)
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeErrorResponse(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func methodNotAllowed(w http.ResponseWriter) {
	writeErrorResponse(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}
