package extractor

import (
	"context"
	"time"
)

// NoopExtractor simulates work without actual processing.
// This is the default behavior matching the current worker implementation.
type NoopExtractor struct {
	delay time.Duration
}

// NewNoopExtractor creates a new no-op extractor with the specified delay.
func NewNoopExtractor(delay time.Duration) *NoopExtractor {
	return &NoopExtractor{delay: delay}
}

// Name returns the extractor type name.
func (e *NoopExtractor) Name() string {
	return "noop"
}

// CanHandle always returns true as this is the fallback extractor.
func (e *NoopExtractor) CanHandle(ctx context.Context, payload string) bool {
	return true
}

// Extract simulates work by waiting for the configured delay.
// Respects context cancellation for graceful shutdown.
func (e *NoopExtractor) Extract(ctx context.Context, jobID string, payload string) (*Result, error) {
	select {
	case <-time.After(e.delay):
		return &Result{
			Success: true,
			Metadata: map[string]interface{}{
				"extractor": e.Name(),
				"job_id":    jobID,
				"simulated": true,
			},
		}, nil
	case <-ctx.Done():
		return &Result{
			Success:  false,
			Error:    ctx.Err(),
			ErrorMsg: ctx.Err().Error(),
		}, ctx.Err()
	}
}
