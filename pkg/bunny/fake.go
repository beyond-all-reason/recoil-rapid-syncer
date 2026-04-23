// SPDX-FileCopyrightText: 2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package bunny

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// FakeStorageZone is an in-memory implementation of StorageZoneOperations
// for use in tests. It is safe for concurrent use.
type FakeStorageZone struct {
	mu    sync.RWMutex
	files map[string][]byte
}

// NewFakeStorageZone creates a new empty FakeStorageZone.
func NewFakeStorageZone() *FakeStorageZone {
	return &FakeStorageZone{files: make(map[string][]byte)}
}

// Seed pre-populates the fake with the given files.
func (f *FakeStorageZone) Seed(files map[string][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range files {
		cp := make([]byte, len(v))
		copy(cp, v)
		f.files[k] = cp
	}
}

// Files returns a copy of all stored files for assertions.
func (f *FakeStorageZone) Files() map[string][]byte {
	f.mu.RLock()
	defer f.mu.RUnlock()
	result := make(map[string][]byte, len(f.files))
	for k, v := range f.files {
		cp := make([]byte, len(v))
		copy(cp, v)
		result[k] = cp
	}
	return result
}

func (f *FakeStorageZone) Upload(_ context.Context, filePath string, contents io.Reader) error {
	data, err := io.ReadAll(contents)
	if err != nil {
		return fmt.Errorf("fake upload: read failed: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[filePath] = data
	return nil
}

func (f *FakeStorageZone) Download(_ context.Context, filePath string) (io.ReadCloser, int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	data, ok := f.files[filePath]
	if !ok {
		return nil, http.StatusNotFound, fmt.Errorf("fake download: file %q not found", filePath)
	}
	return io.NopCloser(bytes.NewReader(data)), http.StatusOK, nil
}

func (f *FakeStorageZone) Delete(_ context.Context, filePath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[filePath]; !ok {
		return fmt.Errorf("fake delete: file %q not found", filePath)
	}
	delete(f.files, filePath)
	return nil
}

func (f *FakeStorageZone) List(_ context.Context, dirPath string) ([]string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	prefix := dirPath
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	seen := make(map[string]struct{})
	var result []string
	for k := range f.files {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := strings.TrimPrefix(k, prefix)
		child := rest
		if idx := strings.Index(rest, "/"); idx >= 0 {
			child = rest[:idx]
		}
		if child != "" {
			if _, ok := seen[child]; !ok {
				seen[child] = struct{}{}
				result = append(result, child)
			}
		}
	}
	if result == nil {
		return []string{}, nil
	}
	return result, nil
}
