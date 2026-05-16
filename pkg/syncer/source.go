// SPDX-FileCopyrightText: 2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"context"
	"errors"
	"io"
)

// Source reads gzipped files from a rapid-repo tree. Paths are relative to
// the repo root and use forward slashes (e.g. "versions.gz",
// "packages/<hash>.sdp", "pool/<xx>/<rest>.gz"). The returned stream is the
// raw gzipped bytes as stored in the repo; callers are responsible for gzip
// decompression.
//
// Implementations must return an error matching ErrNotFound (via errors.Is)
// when a requested path does not exist.
type Source interface {
	Open(ctx context.Context, path string, opts OpenOptions) (io.ReadCloser, error)
}

// OpenOptions configures a single Source.Open call.
type OpenOptions struct {
	// NoCache asks the source to bypass any intermediate cache when
	// fetching this file. HTTP sources translate it to a Cache-Control
	// request header; sources without a cache (e.g. local filesystem)
	// ignore it.
	NoCache bool
}

// ErrNotFound is returned by Source.Open when the requested path does not
// exist. The syncer relies on this to gracefully handle a missing
// versions.gz.
var ErrNotFound = errors.New("syncer: source file not found")
