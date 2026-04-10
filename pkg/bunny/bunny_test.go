// SPDX-FileCopyrightText: 2023 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package bunny

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestClient(serverURL string) *Client {
	c := NewClient("test-access-key")
	c.apiBaseURL = serverURL
	return c
}

func newTestStorageZoneClient(serverURL string) *StorageZoneClient {
	sz := NewStorageZoneClient("testzone", "storage.example.com", "test-storage-key")
	sz.storageBaseURL = serverURL
	return sz
}

// --- StorageZoneClient tests ---

func TestUpload_Success(t *testing.T) {
	body := "hello world"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if r.URL.Path != "/testzone/docs/file.txt" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("AccessKey") != "test-storage-key" {
			t.Errorf("unexpected AccessKey: %s", r.Header.Get("AccessKey"))
		}
		// ReadSeeker should produce a Checksum header
		h := sha256.Sum256([]byte(body))
		expectedChecksum := strings.ToUpper(hex.EncodeToString(h[:]))
		if r.Header.Get("Checksum") != expectedChecksum {
			t.Errorf("unexpected Checksum: got %q, want %q", r.Header.Get("Checksum"), expectedChecksum)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	err := sz.Upload(context.Background(), "docs/file.txt", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
}

func TestUpload_NoChecksum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Checksum") != "" {
			t.Errorf("expected no Checksum header for plain io.Reader, got %q", r.Header.Get("Checksum"))
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	// Use a plain io.Reader (not ReadSeeker) by wrapping
	reader := struct{ io.Reader }{strings.NewReader("data")}
	err := sz.Upload(context.Background(), "file.bin", reader)
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
}

func TestUpload_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	err := sz.Upload(context.Background(), "file.txt", strings.NewReader("data"))
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestDownload_Success(t *testing.T) {
	expected := "file contents here"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(expected))
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	body, statusCode, err := sz.Download(context.Background(), "myfile.txt")
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	defer body.Close()
	if statusCode != http.StatusOK {
		t.Errorf("unexpected status code: %d", statusCode)
	}
	data, _ := io.ReadAll(body)
	if string(data) != expected {
		t.Errorf("unexpected body: got %q, want %q", string(data), expected)
	}
}

func TestDownload_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	_, statusCode, err := sz.Download(context.Background(), "missing.txt")
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
	if statusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", statusCode)
	}
}

func TestDelete_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.Header.Get("AccessKey") != "test-storage-key" {
			t.Errorf("unexpected AccessKey: %s", r.Header.Get("AccessKey"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	err := sz.Delete(context.Background(), "old/file.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestDelete_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	err := sz.Delete(context.Background(), "file.txt")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestList_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.Header.Get("AccessKey") != "test-storage-key" {
			t.Errorf("unexpected AccessKey: %s", r.Header.Get("AccessKey"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]map[string]string{
			{"ObjectName": "file1.txt"},
			{"ObjectName": "file2.txt"},
			{"ObjectName": "subdir"},
		})
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	files, err := sz.List(context.Background(), "mydir")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(files))
	}
	expected := []string{"file1.txt", "file2.txt", "subdir"}
	for i, f := range files {
		if f != expected[i] {
			t.Errorf("files[%d] = %q, want %q", i, f, expected[i])
		}
	}
}

func TestList_Empty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	files, err := sz.List(context.Background(), "emptydir")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected empty slice, got %v", files)
	}
}

func TestList_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	sz := newTestStorageZoneClient(server.URL)
	_, err := sz.List(context.Background(), "dir")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestGetFileUrl(t *testing.T) {
	sz := &StorageZoneClient{
		zoneName:       "myzone",
		storageBaseURL: "https://storage.example.com",
	}

	tests := []struct {
		name     string
		filePath string
		want     string
	}{
		{"simple", "file.txt", "https://storage.example.com/myzone/file.txt"},
		{"nested", "a/b/file.txt", "https://storage.example.com/myzone/a%2Fb/file.txt"},
		{"special chars", "dir/my file.txt", "https://storage.example.com/myzone/dir/my%20file.txt"},
		{"root file", "readme.md", "https://storage.example.com/myzone/readme.md"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sz.getFileUrl(tt.filePath)
			if got != tt.want {
				t.Errorf("getFileUrl(%q) = %q, want %q", tt.filePath, got, tt.want)
			}
		})
	}
}

// --- Client tests ---

func TestEdgeServersIP_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/system/edgeserverlist" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{"1.2.3.4", "5.6.7.8"})
	}))
	defer server.Close()

	c := newTestClient(server.URL)
	ips, err := c.EdgeServersIP(context.Background())
	if err != nil {
		t.Fatalf("EdgeServersIP failed: %v", err)
	}
	if len(ips) != 2 || ips[0] != "1.2.3.4" || ips[1] != "5.6.7.8" {
		t.Errorf("unexpected result: %v", ips)
	}
}

func TestEdgeServersIP_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	c := newTestClient(server.URL)
	_, err := c.EdgeServersIP(context.Background())
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestRegions_Success(t *testing.T) {
	regions := []Region{
		{Id: 1, Name: "EU", RegionCode: "EU", Latitude: 50.0, Longitude: 10.0},
		{Id: 2, Name: "US", RegionCode: "US", Latitude: 40.0, Longitude: -100.0},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/region" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(regions)
	}))
	defer server.Close()

	c := newTestClient(server.URL)
	result, err := c.Regions(context.Background())
	if err != nil {
		t.Fatalf("Regions failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 regions, got %d", len(result))
	}
	if result[0].Name != "EU" || result[1].Name != "US" {
		t.Errorf("unexpected regions: %v", result)
	}
}

func TestStorageZones_Success(t *testing.T) {
	zones := []StorageZone{
		{Id: 1, Name: "zone1", Password: "pass1", StorageHostname: "storage.bunnycdn.com"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("AccessKey") != "test-access-key" {
			t.Errorf("unexpected AccessKey: %s", r.Header.Get("AccessKey"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(zones)
	}))
	defer server.Close()

	c := newTestClient(server.URL)
	result, err := c.StorageZones(context.Background())
	if err != nil {
		t.Fatalf("StorageZones failed: %v", err)
	}
	if len(result) != 1 || result[0].Name != "zone1" {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestStorageZoneByName_Found(t *testing.T) {
	zones := []StorageZone{
		{Id: 1, Name: "alpha"},
		{Id: 2, Name: "beta"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(zones)
	}))
	defer server.Close()

	c := newTestClient(server.URL)
	zone, err := c.StorageZoneByName(context.Background(), "beta")
	if err != nil {
		t.Fatalf("StorageZoneByName failed: %v", err)
	}
	if zone.Id != 2 || zone.Name != "beta" {
		t.Errorf("unexpected zone: %v", zone)
	}
}

func TestStorageZoneByName_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]StorageZone{})
	}))
	defer server.Close()

	c := newTestClient(server.URL)
	_, err := c.StorageZoneByName(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing zone")
	}
}

// --- Utility function tests ---

func TestServerRegionCode(t *testing.T) {
	tests := []struct {
		name    string
		server  string
		want    string
		wantErr bool
	}{
		{"valid", "BunnyCDN-DE1-123", "DE", false},
		{"valid multi-letter", "BunnyCDN-SYD1-42", "SYD", false},
		{"invalid", "nginx/1.0", "", true},
		{"empty", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			h.Set("Server", tt.server)
			got, err := ServerRegionCode(&h)
			if (err != nil) != tt.wantErr {
				t.Errorf("ServerRegionCode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ServerRegionCode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStorageServerRegionCode(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		want    string
		wantErr bool
	}{
		{"valid", "DE-123", "DE", false},
		{"valid multi-letter", "SYD-42", "SYD", false},
		{"invalid", "something-else", "", true},
		{"empty", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			h.Set("CDN-StorageServer", tt.header)
			got, err := StorageServerRegionCode(&h)
			if (err != nil) != tt.wantErr {
				t.Errorf("StorageServerRegionCode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("StorageServerRegionCode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRegionsDistanceKm(t *testing.T) {
	// London to Paris: ~340 km
	london := &Region{Latitude: 51.5074, Longitude: -0.1278}
	paris := &Region{Latitude: 48.8566, Longitude: 2.3522}
	dist := RegionsDistanceKm(london, paris)
	if dist < 300 || dist > 400 {
		t.Errorf("London-Paris distance = %.1f km, expected ~340 km", dist)
	}

	// Same point should be 0
	same := &Region{Latitude: 0, Longitude: 0}
	if d := RegionsDistanceKm(same, same); d != 0 {
		t.Errorf("same point distance = %f, want 0", d)
	}
}
