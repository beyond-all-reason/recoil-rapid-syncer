// SPDX-FileCopyrightText: 2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPSource reads a rapid repo from an HTTP(S) server.
type HTTPSource struct {
	baseURL string
	client  *http.Client
}

// NewHTTPClient returns an *http.Client tuned for parallel rapid syncs.
// Callers should construct one client at process start and share it across
// HTTPSource instances so the connection pool is reused.
func NewHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxConnsPerHost:     75,
			MaxIdleConnsPerHost: 75,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}
}

// NewHTTPSource returns a Source that reads from baseURL using the given
// client. A trailing '/' is appended to baseURL if missing.
func NewHTTPSource(baseURL string, client *http.Client) *HTTPSource {
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	return &HTTPSource{baseURL: baseURL, client: client}
}

func (s *HTTPSource) Open(ctx context.Context, path string, opts OpenOptions) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "recoil-rapid-syncer 1.0")
	if opts.NoCache {
		req.Header.Set("Cache-Control", "no-cache")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, fmt.Errorf("%s: %w", path, ErrNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("http request for %s failed with code: %d", path, resp.StatusCode)
	}
	return resp.Body, nil
}
