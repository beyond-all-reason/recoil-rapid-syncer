// SPDX-FileCopyrightText: 2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPSource_OK(t *testing.T) {
	var gotPath, gotUserAgent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotUserAgent = r.Header.Get("User-Agent")
		w.Write([]byte("hello"))
	}))
	defer srv.Close()

	s := NewHTTPSource(srv.URL+"/repo/", http.DefaultClient)
	rc, err := s.Open(context.Background(), "versions.gz", OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(b) != "hello" {
		t.Errorf("body = %q, want %q", b, "hello")
	}
	if gotPath != "/repo/versions.gz" {
		t.Errorf("path = %q", gotPath)
	}
	if gotUserAgent == "" {
		t.Errorf("User-Agent should be set")
	}
}

func TestHTTPSource_NoCacheOption(t *testing.T) {
	var gotCacheControl string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCacheControl = r.Header.Get("Cache-Control")
		w.Write([]byte("x"))
	}))
	defer srv.Close()

	s := NewHTTPSource(srv.URL+"/", http.DefaultClient)

	rc, err := s.Open(context.Background(), "any/path", OpenOptions{NoCache: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rc.Close()
	if gotCacheControl != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache when NoCache is true", gotCacheControl)
	}

	rc, err = s.Open(context.Background(), "any/path", OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rc.Close()
	if gotCacheControl != "" {
		t.Errorf("Cache-Control = %q, want empty when NoCache is false", gotCacheControl)
	}
}

func TestHTTPSource_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	s := NewHTTPSource(srv.URL+"/", http.DefaultClient)
	_, err := s.Open(context.Background(), "versions.gz", OpenOptions{})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestHTTPSource_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := NewHTTPSource(srv.URL+"/", http.DefaultClient)
	_, err := s.Open(context.Background(), "versions.gz", OpenOptions{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("500 should not be ErrNotFound")
	}
}

func TestHTTPSource_AppendsTrailingSlash(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte("x"))
	}))
	defer srv.Close()

	s := NewHTTPSource(srv.URL+"/repo", http.DefaultClient) // no trailing slash
	rc, err := s.Open(context.Background(), "versions.gz", OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rc.Close()
	if gotPath != "/repo/versions.gz" {
		t.Errorf("path = %q, want /repo/versions.gz", gotPath)
	}
}

func TestHTTPSource_ContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewHTTPSource(srv.URL+"/", http.DefaultClient)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Open(ctx, "versions.gz", OpenOptions{}); err == nil {
		t.Fatal("expected error from canceled context")
	}
}
