package server

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const maxRows = 1_000_000

type DatasetSpecification struct {
	Dataset  DatasetConfig  `json:"dataset"`
	Entities []EntityConfig `json:"entities"`
	Outputs  OutputConfig   `json:"outputs"`
}

type DatasetConfig struct {
	Name               string `json:"name"`
	Seed               int64  `json:"seed"`
	StartDate          string `json:"startDate"`
	EndDate            string `json:"endDate"`
	UserCount          int    `json:"userCount"`
	ActivityMultiplier int    `json:"activityMultiplier"`
}

type EntityConfig struct {
	Name          string                        `json:"name"`
	Count         string                        `json:"count"`
	Relationships map[string]RelationshipConfig `json:"relationships"`
}

type RelationshipConfig struct {
	References  string `json:"references"`
	Cardinality string `json:"cardinality"`
}

type OutputConfig struct {
	Formats           []string `json:"formats"`
	Zip               bool     `json:"zip"`
	PublishToKafka    bool     `json:"publishToKafka"`
	PersistToPostgres bool     `json:"persistToPostgres"`
}

type ValidationResult struct {
	Valid    bool     `json:"valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

type Estimate struct {
	UserRows     int   `json:"userRows"`
	ActivityRows int   `json:"activityRows"`
	TotalRows    int   `json:"totalRows"`
	ApproxBytes  int64 `json:"approxBytes"`
}

type ActivityEvent struct {
	EventID    string `json:"event_id"`
	UserID     string `json:"user_id"`
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
}

func ValidateSpecification(spec DatasetSpecification) ValidationResult {
	result := ValidationResult{Valid: true}
	if strings.TrimSpace(spec.Dataset.Name) == "" {
		result.Errors = append(result.Errors, "dataset.name is required")
	}
	if spec.Dataset.UserCount <= 0 || spec.Dataset.UserCount > maxRows {
		result.Errors = append(result.Errors, "dataset.userCount must be between 1 and 1000000")
	}
	if spec.Dataset.ActivityMultiplier <= 0 {
		result.Errors = append(result.Errors, "dataset.activityMultiplier must be greater than zero")
	}
	start, startErr := time.Parse("2006-01-02", spec.Dataset.StartDate)
	end, endErr := time.Parse("2006-01-02", spec.Dataset.EndDate)
	if startErr != nil || endErr != nil {
		result.Errors = append(result.Errors, "dataset.startDate and dataset.endDate must use YYYY-MM-DD")
	} else if end.Before(start) {
		result.Errors = append(result.Errors, "dataset.endDate must not be before dataset.startDate")
	}
	if spec.Dataset.UserCount > 0 && spec.Dataset.ActivityMultiplier > 0 && estimateRows(spec) > maxRows {
		result.Errors = append(result.Errors, "total generated rows must not exceed 1000000")
	}
	if len(spec.Entities) > 0 {
		seen := map[string]bool{}
		for _, entity := range spec.Entities {
			if entity.Name == "" || seen[entity.Name] {
				result.Errors = append(result.Errors, "entities must have unique non-empty names")
			}
			seen[entity.Name] = true
		}
	}
	result.Valid = len(result.Errors) == 0
	return result
}

func EstimateSpecification(spec DatasetSpecification) Estimate {
	activityRows := spec.Dataset.UserCount * spec.Dataset.ActivityMultiplier
	return Estimate{
		UserRows:     spec.Dataset.UserCount,
		ActivityRows: activityRows,
		TotalRows:    spec.Dataset.UserCount + activityRows,
		ApproxBytes:  int64(spec.Dataset.UserCount)*180 + int64(activityRows)*160,
	}
}

func estimateRows(spec DatasetSpecification) int {
	return spec.Dataset.UserCount + spec.Dataset.UserCount*spec.Dataset.ActivityMultiplier
}

func stableID(seed int64, kind string, index int) string {
	hash := sha256.Sum256(fmt.Appendf(nil, "%d:%s:%d", seed, kind, index))
	return hex.EncodeToString(hash[:16])
}

func Generate(ctx context.Context, spec DatasetSpecification, outputDir string, progress func(int)) ([]string, error) {
	if validation := ValidateSpecification(spec); !validation.Valid {
		return nil, errors.New(strings.Join(validation.Errors, "; "))
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, err
	}
	usersPath := filepath.Join(outputDir, "users.csv")
	activitiesPath := filepath.Join(outputDir, "activity_events.csv")
	eventsPath := filepath.Join(outputDir, "events.jsonl")
	usersFile, err := os.Create(usersPath)
	if err != nil {
		return nil, err
	}
	defer usersFile.Close()
	activitiesFile, err := os.Create(activitiesPath)
	if err != nil {
		return nil, err
	}
	defer activitiesFile.Close()
	eventsFile, err := os.Create(eventsPath)
	if err != nil {
		return nil, err
	}
	defer eventsFile.Close()
	usersWriter := csv.NewWriter(usersFile)
	activitiesWriter := csv.NewWriter(activitiesFile)
	if err := usersWriter.Write([]string{"id", "email", "country", "created_at"}); err != nil {
		return nil, err
	}
	if err := activitiesWriter.Write([]string{"event_id", "user_id", "event_type", "occurred_at"}); err != nil {
		return nil, err
	}
	random := rand.New(rand.NewSource(spec.Dataset.Seed))
	countries := []string{"US", "GB", "IN", "CA"}
	eventTypes := []string{"login", "page_view", "purchase", "logout"}
	start, _ := time.Parse("2006-01-02", spec.Dataset.StartDate)
	days := int(specDays(spec))
	for userIndex := 0; userIndex < spec.Dataset.UserCount; userIndex++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		userID := stableID(spec.Dataset.Seed, "user", userIndex)
		createdAt := start.AddDate(0, 0, random.Intn(days+1)).UTC().Format(time.RFC3339)
		country := countries[random.Intn(len(countries))]
		if err := usersWriter.Write([]string{userID, fmt.Sprintf("user%d@example.test", userIndex+1), country, createdAt}); err != nil {
			return nil, err
		}
		for activityIndex := 0; activityIndex < spec.Dataset.ActivityMultiplier; activityIndex++ {
			eventIndex := userIndex*spec.Dataset.ActivityMultiplier + activityIndex
			eventID := stableID(spec.Dataset.Seed, "event", eventIndex)
			occurredAt := start.AddDate(0, 0, random.Intn(days+1)).UTC().Format(time.RFC3339)
			eventType := eventTypes[random.Intn(len(eventTypes))]
			if err := activitiesWriter.Write([]string{eventID, userID, eventType, occurredAt}); err != nil {
				return nil, err
			}
			if err := json.NewEncoder(eventsFile).Encode(ActivityEvent{eventID, userID, eventType, occurredAt}); err != nil {
				return nil, err
			}
		}
		progress((userIndex + 1) * 100 / spec.Dataset.UserCount)
	}
	usersWriter.Flush()
	activitiesWriter.Flush()
	if err := usersWriter.Error(); err != nil {
		return nil, err
	}
	if err := activitiesWriter.Error(); err != nil {
		return nil, err
	}
	paths := []string{usersPath, activitiesPath, eventsPath}
	if spec.Outputs.Zip {
		zipPath := filepath.Join(outputDir, "dataset.zip")
		if err := zipFiles(zipPath, paths); err != nil {
			return nil, err
		}
		paths = append(paths, zipPath)
	}
	return paths, nil
}

func specDays(spec DatasetSpecification) int {
	start, _ := time.Parse("2006-01-02", spec.Dataset.StartDate)
	end, _ := time.Parse("2006-01-02", spec.Dataset.EndDate)
	return int(end.Sub(start).Hours() / 24)
}

func zipFiles(destination string, paths []string) error {
	file, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer file.Close()
	archive := zip.NewWriter(file)
	defer archive.Close()
	for _, path := range paths {
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		entry, err := archive.Create(filepath.Base(path))
		if err == nil {
			_, err = io.Copy(entry, input)
		}
		input.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

type Job struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Progress  int       `json:"progress"`
	Estimate  Estimate  `json:"estimate"`
	Exports   []string  `json:"exports,omitempty"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

type JobStore struct {
	mu     sync.RWMutex
	jobs   map[string]*Job
	cancel map[string]context.CancelFunc
}

func NewJobStore() *JobStore {
	return &JobStore{jobs: make(map[string]*Job), cancel: make(map[string]context.CancelFunc)}
}

func (store *JobStore) Get(id string) (*Job, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	job, ok := store.jobs[id]
	if !ok {
		return nil, false
	}
	copy := *job
	copy.Exports = append([]string(nil), job.Exports...)
	return &copy, true
}

func (store *JobStore) Put(job *Job) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.jobs[job.ID] = job
}

func (store *JobStore) Update(id string, update func(*Job)) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	job, ok := store.jobs[id]
	if !ok {
		return false
	}
	update(job)
	return true
}

func (store *JobStore) SetCancel(id string, cancel context.CancelFunc) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.cancel[id] = cancel
}

func (store *JobStore) Cancel(id string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	cancel, ok := store.cancel[id]
	if ok {
		cancel()
	}
	return ok
}

func jobID(spec DatasetSpecification) string {
	return stableID(spec.Dataset.Seed, spec.Dataset.Name, spec.Dataset.UserCount)
}
