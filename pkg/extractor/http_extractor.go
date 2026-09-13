package extractor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPPayload represents the expected structure for HTTP extractor jobs.
type HTTPPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    interface{}       `json:"body,omitempty"`
	Timeout int               `json:"timeout,omitempty"` // seconds, defaults to 30
}

// HTTPExtractor makes HTTP calls based on job payload.
// Useful for webhook-style jobs or API calls.
type HTTPExtractor struct {
	client *http.Client
}

// NewHTTPExtractor creates a new HTTP extractor with the given client.
// If client is nil, a default client with 30s timeout is used.
func NewHTTPExtractor(client *http.Client) *HTTPExtractor {
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
	return &HTTPExtractor{client: client}
}

// Name returns the extractor type name.
func (e *HTTPExtractor) Name() string {
	return "http"
}

// CanHandle checks if the payload contains a URL field indicating HTTP job.
func (e *HTTPExtractor) CanHandle(ctx context.Context, payload string) bool {
	var p HTTPPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return false
	}
	return p.URL != ""
}

// Extract executes an HTTP request based on the job payload.
func (e *HTTPExtractor) Extract(ctx context.Context, jobID string, payload string) (*Result, error) {
	var p HTTPPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return &Result{
			Success:  false,
			Error:    err,
			ErrorMsg: fmt.Sprintf("invalid payload format: %v", err),
		}, nil
	}

	if p.URL == "" {
		return &Result{
			Success:  false,
			ErrorMsg: "url is required",
		}, nil
	}

	// Default method
	if p.Method == "" {
		p.Method = http.MethodGet
	}
	p.Method = strings.ToUpper(p.Method)

	// Build request body
	var bodyReader io.Reader
	if p.Body != nil {
		bodyBytes, err := json.Marshal(p.Body)
		if err != nil {
			return &Result{
				Success:  false,
				Error:    err,
				ErrorMsg: fmt.Sprintf("failed to marshal body: %v", err),
			}, nil
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, p.Method, p.URL, bodyReader)
	if err != nil {
		return &Result{
			Success:  false,
			Error:    err,
			ErrorMsg: fmt.Sprintf("failed to create request: %v", err),
		}, nil
	}

	// Set default content type for requests with body
	if p.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	// Set custom headers
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}

	// Execute request
	resp, err := e.client.Do(req)
	if err != nil {
		return &Result{
			Success:  false,
			Error:    err,
			ErrorMsg: fmt.Sprintf("request failed: %v", err),
		}, nil
	}
	defer resp.Body.Close()

	// Read response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &Result{
			Success:  false,
			Error:    err,
			ErrorMsg: fmt.Sprintf("failed to read response: %v", err),
		}, nil
	}

	success := resp.StatusCode >= 200 && resp.StatusCode < 300

	return &Result{
		Success: success,
		Output:  string(respBody),
		Metadata: map[string]interface{}{
			"extractor":   e.Name(),
			"job_id":      jobID,
			"status_code": resp.StatusCode,
			"url":         p.URL,
			"method":      p.Method,
		},
	}, nil
}
