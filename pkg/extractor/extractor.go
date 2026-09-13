// Package extractor provides a pluggable mechanism for processing job payloads.
// Workers use extractors to delegate job processing based on payload type.
package extractor

import (
	"context"
	"sync"
)

// Result represents the outcome of job extraction/processing.
type Result struct {
	Success  bool                   `json:"success"`
	Output   interface{}            `json:"output,omitempty"`
	Error    error                  `json:"-"`
	ErrorMsg string                 `json:"error,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// Extractor defines the interface for processing job payloads.
// Different extractors can handle different types of jobs (HTTP calls, shell commands, etc.)
type Extractor interface {
	// Name returns the extractor type name (e.g., "http", "shell", "noop")
	Name() string

	// CanHandle checks if this extractor can process the given payload.
	// Typically examines the payload structure to determine compatibility.
	CanHandle(ctx context.Context, payload string) bool

	// Extract processes the job payload and returns the result.
	// The jobID is provided for logging and tracking purposes.
	Extract(ctx context.Context, jobID string, payload string) (*Result, error)
}

// Registry holds registered extractors and provides lookup functionality.
// It selects the appropriate extractor for a given payload.
type Registry struct {
	mu         sync.RWMutex
	extractors []Extractor
	fallback   Extractor
}

// NewRegistry creates a new extractor registry.
func NewRegistry() *Registry {
	return &Registry{
		extractors: make([]Extractor, 0),
	}
}

// Register adds an extractor to the registry.
// Extractors are checked in registration order when matching payloads.
func (r *Registry) Register(e Extractor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extractors = append(r.extractors, e)
}

// SetFallback sets the fallback extractor for unmatched payloads.
// If no registered extractor can handle a payload, the fallback is used.
func (r *Registry) SetFallback(e Extractor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fallback = e
}

// GetExtractor returns the appropriate extractor for a payload.
// Returns the first registered extractor that can handle the payload,
// or the fallback if no match is found. Returns nil if no fallback is set.
func (r *Registry) GetExtractor(ctx context.Context, payload string) Extractor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, e := range r.extractors {
		if e.CanHandle(ctx, payload) {
			return e
		}
	}
	return r.fallback
}

// Process finds the appropriate extractor and processes the job.
// Returns an error if no extractor is found or if extraction fails.
func (r *Registry) Process(ctx context.Context, jobID, payload string) (*Result, error) {
	extractor := r.GetExtractor(ctx, payload)
	if extractor == nil {
		return &Result{
			Success:  false,
			ErrorMsg: "no extractor found for payload",
		}, nil
	}
	return extractor.Extract(ctx, jobID, payload)
}
