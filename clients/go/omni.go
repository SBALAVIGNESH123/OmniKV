// Package omniclient provides a Go client for OmniKV's REST API.
//
// Usage:
//
//	client := omniclient.New("https://localhost:8443")
//	err := client.Set(ctx, "users/1", `{"name":"Alice"}`)
//	val, err := client.Get(ctx, "users/1")
package omniclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Client is a thread-safe OmniKV REST API client.
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
}

// New creates a new OmniKV client.
func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
				MaxIdleConnsPerHost: 10,
				ForceAttemptHTTP2:   true,
			},
		},
	}
}

// WithToken sets the JWT bearer token for authenticated requests.
func (c *Client) WithToken(token string) *Client {
	c.token = token
	return c
}

// APIResponse is the standard response envelope.
type APIResponse struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   *string         `json:"error,omitempty"`
}

// Get retrieves a key's value.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	resp, err := c.doRequest(ctx, "GET", "/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return "", err
	}
	var kv struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(resp.Data, &kv); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	return kv.Value, nil
}

// Set writes a key-value pair.
func (c *Client) Set(ctx context.Context, key, value string) (uint64, error) {
	body := map[string]string{"key": key, "value": value}
	resp, err := c.doRequest(ctx, "POST", "/kv", body)
	if err != nil {
		return 0, err
	}
	var result struct {
		Seq uint64 `json:"seq"`
	}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return 0, fmt.Errorf("decode: %w", err)
	}
	return result.Seq, nil
}

// Delete removes a key.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.doRequest(ctx, "DELETE", "/kv/"+url.PathEscape(key), nil)
	return err
}

// Scan returns all keys in the range [start, end).
func (c *Client) Scan(ctx context.Context, start, end string, limit int) ([]KeyValue, error) {
	path := fmt.Sprintf("/scan?start=%s&end=%s&limit=%d",
		url.QueryEscape(start), url.QueryEscape(end), limit)
	resp, err := c.doRequest(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	var items []KeyValue
	if err := json.Unmarshal(resp.Data, &items); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return items, nil
}

// Batch executes multiple operations atomically.
func (c *Client) Batch(ctx context.Context, ops []BatchOp) (uint64, error) {
	body := map[string]interface{}{"operations": ops}
	resp, err := c.doRequest(ctx, "POST", "/batch", body)
	if err != nil {
		return 0, err
	}
	var result struct {
		Seq uint64 `json:"seq"`
	}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return 0, fmt.Errorf("decode: %w", err)
	}
	return result.Seq, nil
}

// Health checks the server health.
func (c *Client) Health(ctx context.Context) (*HealthStatus, error) {
	resp, err := c.doRequest(ctx, "GET", "/health", nil)
	if err != nil {
		return nil, err
	}
	var status HealthStatus
	if err := json.Unmarshal(resp.Data, &status); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &status, nil
}

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// BatchOp represents a batch operation.
type BatchOp struct {
	Op    string  `json:"op"`
	Key   string  `json:"key"`
	Value *string `json:"value,omitempty"`
}

// HealthStatus represents the server health response.
type HealthStatus struct {
	Status       string `json:"status"`
	Version      string `json:"version"`
	UptimeSecs   uint64 `json:"uptime_secs"`
	SSTableCount int    `json:"sstable_count"`
}

func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}) (*APIResponse, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}

	var apiResp APIResponse
	if err := json.Unmarshal(respBody, &apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w (body: %s)", err, string(respBody))
	}
	if !apiResp.Success {
		errMsg := "unknown error"
		if apiResp.Error != nil {
			errMsg = *apiResp.Error
		}
		return nil, fmt.Errorf("api error: %s", errMsg)
	}
	return &apiResp, nil
}
