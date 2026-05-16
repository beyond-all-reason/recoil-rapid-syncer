// SPDX-FileCopyrightText: 2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/bunny"
)

// --- fixture helpers -------------------------------------------------------

type fakeEntry struct {
	name    string
	content []byte
}

type fakeArchive struct {
	tag     string
	entries []fakeEntry
}

// build returns versions.gz bytes, a map of archive-hash -> archive .sdp gzip
// bytes, and a map of pool-file-path -> gzipped content.
func buildFixture(t *testing.T, archives []fakeArchive) (versionsGz []byte, sdpByHash map[string][]byte, poolFiles map[string][]byte) {
	t.Helper()
	sdpByHash = make(map[string][]byte)
	poolFiles = make(map[string][]byte)

	type archMeta struct {
		tag, hash string
	}
	var metas []archMeta

	for _, a := range archives {
		var sdp bytes.Buffer
		archHash := md5.New()
		for _, e := range a.entries {
			if len(e.name) > 255 {
				t.Fatalf("entry name too long: %s", e.name)
			}
			entryMd5 := md5.Sum(e.content)
			sdp.WriteByte(byte(len(e.name)))
			sdp.WriteString(e.name)
			sdp.Write(entryMd5[:])
			if err := binary.Write(&sdp, binary.BigEndian, crc32.ChecksumIEEE(e.content)); err != nil {
				t.Fatalf("write crc32: %v", err)
			}
			if err := binary.Write(&sdp, binary.BigEndian, uint32(len(e.content))); err != nil {
				t.Fatalf("write size: %v", err)
			}

			nameMd5 := md5.Sum([]byte(e.name))
			archHash.Write(nameMd5[:])
			archHash.Write(entryMd5[:])

			entryHashHex := hex.EncodeToString(entryMd5[:])
			poolFiles[poolFileFromHash(entryHashHex)] = gzipBytes(t, e.content)
		}
		archHashHex := hex.EncodeToString(archHash.Sum(nil))
		sdpByHash[archHashHex] = gzipBytes(t, sdp.Bytes())
		metas = append(metas, archMeta{tag: a.tag, hash: archHashHex})
	}

	var csvBuf bytes.Buffer
	w := csv.NewWriter(&csvBuf)
	for _, m := range metas {
		// Format: tag,hash,depends,name — parser only uses first two but requires >= 4 fields.
		if err := w.Write([]string{m.tag, m.hash, "", m.tag}); err != nil {
			t.Fatalf("csv write: %v", err)
		}
	}
	w.Flush()
	versionsGz = gzipBytes(t, csvBuf.Bytes())
	return
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// fakeSource implements Source from an in-memory file map and counts per-path
// hits so tests can assert on fetch behavior.
type pathStats struct {
	hits        atomic.Int64
	noCacheHits atomic.Int64
}

type fakeSource struct {
	files map[string][]byte
	stats map[string]*pathStats
}

func newFakeSource(versionsGz []byte, sdps, pool map[string][]byte) *fakeSource {
	files := map[string][]byte{}
	if versionsGz != nil {
		files["versions.gz"] = versionsGz
	}
	for h, b := range sdps {
		files["packages/"+h+".sdp"] = b
	}
	for p, b := range pool {
		files[p] = b
	}
	stats := make(map[string]*pathStats, len(files))
	for k := range files {
		stats[k] = &pathStats{}
	}
	return &fakeSource{files: files, stats: stats}
}

func (s *fakeSource) Open(_ context.Context, path string, opts OpenOptions) (io.ReadCloser, error) {
	b, ok := s.files[path]
	if !ok {
		return nil, fmt.Errorf("%s: %w", path, ErrNotFound)
	}
	st := s.stats[path]
	st.hits.Add(1)
	if opts.NoCache {
		st.noCacheHits.Add(1)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *fakeSource) hitCount(path string) int64 {
	if st, ok := s.stats[path]; ok {
		return st.hits.Load()
	}
	return 0
}

func (s *fakeSource) noCacheCount(path string) int64 {
	if st, ok := s.stats[path]; ok {
		return st.noCacheHits.Load()
	}
	return 0
}

// --- tests -----------------------------------------------------------------

func TestSync_EmptyDestination(t *testing.T) {
	archives := []fakeArchive{{
		tag: "byar:test",
		entries: []fakeEntry{
			{name: "maps/m1.sd7", content: []byte("map one contents")},
			{name: "base/game.sdz", content: []byte("game contents")},
		},
	}}
	versionsGz, sdpByHash, poolFiles := buildFixture(t, archives)
	src := newFakeSource(versionsGz, sdpByHash, poolFiles)

	dst := bunny.NewFakeStorageZone()
	rs := NewRapidSyncer(dst)

	n, err := rs.Sync(context.Background(), src, "byar")
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if n != 1 {
		t.Errorf("synced count = %d, want 1", n)
	}

	got := dst.Files()
	if _, ok := got["byar/versions.gz"]; !ok {
		t.Errorf("missing byar/versions.gz in dest")
	}
	for h := range sdpByHash {
		key := "byar/packages/" + h + ".sdp"
		if _, ok := got[key]; !ok {
			t.Errorf("missing %s in dest", key)
		}
		if got := src.noCacheCount("packages/" + h + ".sdp"); got != 0 {
			t.Errorf("archive %s should not request NoCache: %d", h, got)
		}
	}
	for p := range poolFiles {
		key := "byar/" + p
		if _, ok := got[key]; !ok {
			t.Errorf("missing %s in dest", key)
		}
		if got := src.noCacheCount(p); got != 0 {
			t.Errorf("pool %s should not request NoCache: %d", p, got)
		}
	}
}

func TestSync_AlreadyInSync(t *testing.T) {
	archives := []fakeArchive{{
		tag:     "byar:test",
		entries: []fakeEntry{{name: "file.sd7", content: []byte("content")}},
	}}
	versionsGz, sdpByHash, poolFiles := buildFixture(t, archives)
	src := newFakeSource(versionsGz, sdpByHash, poolFiles)

	dst := bunny.NewFakeStorageZone()
	seed := map[string][]byte{"byar/versions.gz": versionsGz}
	for h, b := range sdpByHash {
		seed["byar/packages/"+h+".sdp"] = b
	}
	for p, b := range poolFiles {
		seed["byar/"+p] = b
	}
	dst.Seed(seed)

	rs := NewRapidSyncer(dst)
	n, err := rs.Sync(context.Background(), src, "byar")
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if n != 0 {
		t.Errorf("synced count = %d, want 0", n)
	}
	if got := src.hitCount("versions.gz"); got != 1 {
		t.Errorf("source versions.gz hits = %d, want 1", got)
	}
	if got := src.noCacheCount("versions.gz"); got != 1 {
		t.Errorf("versions.gz must be fetched with NoCache: noCacheHits = %d, want 1", got)
	}
	for h := range sdpByHash {
		if got := src.hitCount("packages/" + h + ".sdp"); got != 0 {
			t.Errorf("unexpected fetch of archive %s: %d", h, got)
		}
	}
	for p := range poolFiles {
		if got := src.hitCount(p); got != 0 {
			t.Errorf("unexpected fetch of pool %s: %d", p, got)
		}
	}
}

func TestSync_PartialDedupByPoolListing(t *testing.T) {
	shared := []byte("shared pool entry")
	sharedMd5 := md5.Sum(shared)
	sharedHash := hex.EncodeToString(sharedMd5[:])

	archives := []fakeArchive{
		{
			tag: "byar:a",
			entries: []fakeEntry{
				{name: "shared.sdz", content: shared},
				{name: "unique-a.sdz", content: []byte("only in a")},
			},
		},
		{
			tag: "byar:b",
			entries: []fakeEntry{
				{name: "shared.sdz", content: shared},
				{name: "unique-b.sdz", content: []byte("only in b")},
			},
		},
	}
	versionsGz, sdpByHash, poolFiles := buildFixture(t, archives)
	src := newFakeSource(versionsGz, sdpByHash, poolFiles)

	// Seed the destination with the shared pool entry already present.
	dst := bunny.NewFakeStorageZone()
	sharedPoolPath := poolFileFromHash(sharedHash)
	dst.Seed(map[string][]byte{
		"byar/" + sharedPoolPath: poolFiles[sharedPoolPath],
	})

	rs := NewRapidSyncer(dst)
	n, err := rs.Sync(context.Background(), src, "byar")
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if n != 2 {
		t.Errorf("synced count = %d, want 2", n)
	}

	// The shared pool entry must not have been re-fetched from source.
	if got := src.hitCount(sharedPoolPath); got != 0 {
		t.Errorf("shared pool entry was fetched %d times, want 0", got)
	}

	// All expected pool files are present on dest.
	got := dst.Files()
	for p := range poolFiles {
		if _, ok := got["byar/"+p]; !ok {
			t.Errorf("missing %s in dest", p)
		}
	}
}

func TestSync_MissingDestVersions404(t *testing.T) {
	archives := []fakeArchive{{
		tag:     "byar:test",
		entries: []fakeEntry{{name: "f.sd7", content: []byte("x")}},
	}}
	versionsGz, sdpByHash, poolFiles := buildFixture(t, archives)
	src := newFakeSource(versionsGz, sdpByHash, poolFiles)

	dst := bunny.NewFakeStorageZone() // fully empty, Download yields 404
	rs := NewRapidSyncer(dst)

	n, err := rs.Sync(context.Background(), src, "byar")
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if n != 1 {
		t.Errorf("synced count = %d, want 1", n)
	}
}

func TestSync_CorruptedSourceArchive(t *testing.T) {
	archives := []fakeArchive{{
		tag:     "byar:test",
		entries: []fakeEntry{{name: "f.sd7", content: []byte("hello")}},
	}}
	versionsGz, sdpByHash, poolFiles := buildFixture(t, archives)

	// Corrupt the single SDP: flip a byte in its decompressed content by
	// rebuilding the gzip around tampered bytes.
	var archHash string
	for h := range sdpByHash {
		archHash = h
	}
	gzr, err := gzip.NewReader(bytes.NewReader(sdpByHash[archHash]))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	var plain bytes.Buffer
	if _, err := plain.ReadFrom(gzr); err != nil {
		t.Fatalf("read gzip: %v", err)
	}
	b := plain.Bytes()
	// Flip the md5-of-content region of the first entry (offset: 1 + len(name)).
	b[1+len("f.sd7")] ^= 0xff
	sdpByHash[archHash] = gzipBytes(t, b)

	src := newFakeSource(versionsGz, sdpByHash, poolFiles)
	dst := bunny.NewFakeStorageZone()
	rs := NewRapidSyncer(dst)

	_, err = rs.Sync(context.Background(), src, "byar")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "hash") {
		t.Errorf("error should mention hash mismatch, got: %v", err)
	}
}

func TestSync_SourceVersions404(t *testing.T) {
	src := newFakeSource(nil, nil, nil) // nothing served; everything ErrNotFound
	dst := bunny.NewFakeStorageZone()
	rs := NewRapidSyncer(dst)

	_, err := rs.Sync(context.Background(), src, "byar")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "empty/not-found") {
		t.Errorf("error should mention empty/not-found, got: %v", err)
	}
}

func TestSync_PoolListingSeesPreseededOrphan(t *testing.T) {
	// An orphan pool entry on the destination (not referenced by source
	// versions.gz) must still be picked up by the listing and cause any
	// source entry sharing that hash to be skipped.
	orphanContent := []byte("orphan data")
	orphanMd5 := md5.Sum(orphanContent)
	orphanHash := hex.EncodeToString(orphanMd5[:])
	orphanPoolPath := poolFileFromHash(orphanHash)

	archives := []fakeArchive{{
		tag: "byar:test",
		entries: []fakeEntry{
			{name: "file.sdz", content: orphanContent}, // same content as orphan
			{name: "other.sdz", content: []byte("other")},
		},
	}}
	versionsGz, sdpByHash, poolFiles := buildFixture(t, archives)
	src := newFakeSource(versionsGz, sdpByHash, poolFiles)

	dst := bunny.NewFakeStorageZone()
	dst.Seed(map[string][]byte{
		"byar/" + orphanPoolPath: gzipBytes(t, orphanContent),
	})

	rs := NewRapidSyncer(dst)
	_, err := rs.Sync(context.Background(), src, "byar")
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	if got := src.hitCount(orphanPoolPath); got != 0 {
		t.Errorf("orphan pool entry was re-fetched %d times, want 0", got)
	}
}

// --- sanity ---------------------------------------------------------------

func TestPoolFileRegex(t *testing.T) {
	// Cheap guard against the internal pool-file regex regressing.
	cases := []struct {
		name string
		ok   bool
	}{
		{"000102030405060708090a0b0c0d0e.gz", true},
		{"000102030405060708090a0b0c0d0e0f.gz", false}, // 32 hex, too long
		{"000102030405060708090a0b0c0d.gz", false},     // too short
		{"000102030405060708090a0b0c0d0G.gz", false},   // invalid char
		{"foo", false},
	}
	for _, c := range cases {
		if got := poolFileRegex.MatchString(c.name); got != c.ok {
			t.Errorf("match(%q) = %v, want %v", c.name, got, c.ok)
		}
	}
}
