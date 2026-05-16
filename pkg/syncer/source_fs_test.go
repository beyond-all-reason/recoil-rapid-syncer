// SPDX-FileCopyrightText: 2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestFSSource_ReadsFile(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "versions.gz"), []byte("raw bytes"))

	s := NewFSSource(root)
	rc, err := s.Open(context.Background(), "versions.gz", OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(b) != "raw bytes" {
		t.Errorf("body = %q, want %q", b, "raw bytes")
	}
}

func TestFSSource_MissingFile(t *testing.T) {
	s := NewFSSource(t.TempDir())
	_, err := s.Open(context.Background(), "versions.gz", OpenOptions{})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestFSSource_NestedPath(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "pool", "ab", "cd.gz"), []byte("x"))

	s := NewFSSource(root)
	rc, err := s.Open(context.Background(), "pool/ab/cd.gz", OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rc.Close()
}

func TestFSSource_RejectsTraversal(t *testing.T) {
	s := NewFSSource(t.TempDir())
	cases := []string{"../etc/passwd", "pool/../../oops", ".."}
	for _, c := range cases {
		if _, err := s.Open(context.Background(), c, OpenOptions{}); err == nil {
			t.Errorf("expected error for traversal path %q", c)
		}
	}
}

func TestFSSource_ContextCanceled(t *testing.T) {
	s := NewFSSource(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Open(ctx, "versions.gz", OpenOptions{}); err == nil {
		t.Fatal("expected error from canceled context")
	}
}
