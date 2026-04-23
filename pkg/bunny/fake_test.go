// SPDX-FileCopyrightText: 2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package bunny

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
)

// Compile-time check that FakeStorageZone implements StorageZoneOperations.
var _ StorageZoneOperations = (*FakeStorageZone)(nil)

func TestFake_UploadDownloadRoundtrip(t *testing.T) {
	f := NewFakeStorageZone()
	ctx := context.Background()

	err := f.Upload(ctx, "dir/file.txt", strings.NewReader("hello"))
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	body, status, err := f.Download(ctx, "dir/file.txt")
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	defer body.Close()
	if status != 200 {
		t.Errorf("unexpected status: %d", status)
	}
	data, _ := io.ReadAll(body)
	if string(data) != "hello" {
		t.Errorf("got %q, want %q", string(data), "hello")
	}
}

func TestFake_ListAfterUploads(t *testing.T) {
	f := NewFakeStorageZone()
	ctx := context.Background()

	f.Upload(ctx, "docs/a.txt", strings.NewReader("a"))
	f.Upload(ctx, "docs/b.txt", strings.NewReader("b"))
	f.Upload(ctx, "docs/sub/c.txt", strings.NewReader("c"))
	f.Upload(ctx, "other/d.txt", strings.NewReader("d"))

	files, err := f.List(ctx, "docs")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	sort.Strings(files)
	if len(files) != 3 {
		t.Fatalf("expected 3 entries, got %d: %v", len(files), files)
	}
	expected := []string{"a.txt", "b.txt", "sub"}
	for i, f := range files {
		if f != expected[i] {
			t.Errorf("files[%d] = %q, want %q", i, f, expected[i])
		}
	}
}

func TestFake_DeleteThenDownloadFails(t *testing.T) {
	f := NewFakeStorageZone()
	ctx := context.Background()

	f.Upload(ctx, "file.txt", strings.NewReader("data"))
	err := f.Delete(ctx, "file.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, _, err = f.Download(ctx, "file.txt")
	if err == nil {
		t.Fatal("expected error downloading deleted file")
	}
}

func TestFake_DeleteNonexistent(t *testing.T) {
	f := NewFakeStorageZone()
	err := f.Delete(context.Background(), "nope.txt")
	if err == nil {
		t.Fatal("expected error deleting nonexistent file")
	}
}

func TestFake_Seed(t *testing.T) {
	f := NewFakeStorageZone()
	f.Seed(map[string][]byte{
		"a.txt": []byte("aaa"),
		"b.txt": []byte("bbb"),
	})

	files := f.Files()
	if len(files) != 2 {
		t.Fatalf("expected 2 files, got %d", len(files))
	}
	if string(files["a.txt"]) != "aaa" {
		t.Errorf("a.txt = %q, want %q", string(files["a.txt"]), "aaa")
	}

	// Verify Download works for seeded files
	body, _, err := f.Download(context.Background(), "b.txt")
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}
	defer body.Close()
	data, _ := io.ReadAll(body)
	if string(data) != "bbb" {
		t.Errorf("got %q, want %q", string(data), "bbb")
	}
}

func TestFake_ConcurrentAccess(t *testing.T) {
	f := NewFakeStorageZone()
	ctx := context.Background()
	var wg sync.WaitGroup

	// Concurrent uploads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := strings.Repeat("x", i%10) + ".txt"
			f.Upload(ctx, key, strings.NewReader("data"))
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.List(ctx, "")
		}()
	}

	wg.Wait()
	// If we got here without a race detector complaint, the test passes.
}
