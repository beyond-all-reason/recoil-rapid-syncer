// SPDX-FileCopyrightText: 2023 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package sfcache

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type Cache[T any] struct {
	mu         sync.RWMutex
	sfg        singleflight.Group
	lastUpdate time.Time
	value      T
	Timeout    time.Duration
}

func (c *Cache[T]) Get(ctx context.Context, updater func() (T, error)) (T, error) {
	var res T
	fresh := false
	c.mu.RLock()
	if c.lastUpdate.Add(c.Timeout).After(time.Now()) {
		res = c.value
		fresh = true
	}
	c.mu.RUnlock()
	if fresh {
		return res, nil
	}

	resCh := c.sfg.DoChan("", func() (interface{}, error) {
		val, err := updater()
		if err == nil {
			c.mu.Lock()
			c.value = val
			c.lastUpdate = time.Now()
			c.mu.Unlock()
		}
		return val, err
	})

	select {
	case res := <-resCh:
		if res.Val == nil {
			var v T
			return v, res.Err
		} else {
			return res.Val.(T), res.Err
		}
	case <-ctx.Done():
		return c.value, ctx.Err()
	}
}
