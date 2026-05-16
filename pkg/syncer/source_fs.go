// SPDX-FileCopyrightText: 2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// FSSource reads a rapid repo from a local filesystem directory, laid out
// with the same structure as the HTTP server (versions.gz,
// packages/<hash>.sdp, pool/<xx>/<rest>.gz).
type FSSource struct {
	root string
}

// NewFSSource returns a Source rooted at the given local directory.
func NewFSSource(root string) *FSSource {
	return &FSSource{root: root}
}

func (s *FSSource) Open(ctx context.Context, p string, _ OpenOptions) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	local := filepath.FromSlash(p)
	if !filepath.IsLocal(local) {
		return nil, fmt.Errorf("invalid source path: %q", p)
	}
	f, err := os.Open(filepath.Join(s.root, local))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%s: %w", p, ErrNotFound)
		}
		return nil, err
	}
	return f, nil
}
