package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"time"

	"github.com/p2004a/spring-rapid-syncer/pkg/bunny"
	"golang.org/x/sync/errgroup"
)

type versionsGzFile struct {
	contents      []byte
	lastModified  time.Time
	etag          string
	storageServer string
}

type byServerName []*versionsGzFile

func (s byServerName) Len() int           { return len(s) }
func (s byServerName) Less(i, j int) bool { return s[i].storageServer < s[i].storageServer }
func (s byServerName) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type byLastModified []*versionsGzFile

func (s byLastModified) Len() int           { return len(s) }
func (s byLastModified) Less(i, j int) bool { return s[i].lastModified.Before(s[j].lastModified) }
func (s byLastModified) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *Server) fetchVersionsGZ(ctx context.Context, ip, expectedSS string) (*versionsGzFile, error) {
	client := httpClientForAddr(ip+":443", time.Second*20)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.versionsGzUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "spring-rapid-syncer/latestreplicated 1.0")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to ip %s failed with: %w", ip, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("downloading versions.gz failed: %w", err)
	}

	storageServerRegion, err := bunny.StorageServerRegionCode(&resp.Header)
	if err != nil {
		return nil, err
	}
	if storageServerRegion != expectedSS {
		return nil, fmt.Errorf("got response from unexpected storage server from edge %s: %s, expected %s", ip, storageServerRegion, expectedSS)
	}

	modified, err := http.ParseTime(resp.Header.Get("Last-Modified"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse Last-Modified: %w", err)
	}

	return &versionsGzFile{
		contents:      body,
		lastModified:  modified,
		etag:          resp.Header.Get("ETag"),
		storageServer: expectedSS,
	}, nil
}

func (s *Server) fetchLatestSyncedVersionsGZ(ctx context.Context, sm ServersMap) ([]*versionsGzFile, error) {
	grp, subCtx := errgroup.WithContext(ctx)
	allVersionsGz := make([]*versionsGzFile, len(sm))
	i := 0
	for storageServer, ips := range sm {
		ip := ips[rand.Intn(len(ips))]
		id := i
		ss := storageServer
		grp.Go(func() error {
			var err error
			allVersionsGz[id], err = s.fetchVersionsGZ(subCtx, ip, ss)
			return err
		})
		i++
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}
	sort.Sort(byLastModified(allVersionsGz))
	return allVersionsGz, nil
}

func (s *Server) sfFetchLatestSyncedVersionsGZ(ctx context.Context, sm ServersMap) ([]*versionsGzFile, error) {
	return s.versionsGzCache.Get(ctx, func() ([]*versionsGzFile, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		return s.fetchLatestSyncedVersionsGZ(ctx, sm)
	})
}

func (s *Server) HandleVersionsGz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		log.Printf("Got %s, not GET/HEAD request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	serversMap, err := s.sfFetchEdgeServersMap(r.Context())
	if err != nil {
		log.Printf("Failed to get edge server map: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	versionsGz, err := s.sfFetchLatestSyncedVersionsGZ(r.Context(), serversMap)
	if err != nil {
		log.Printf("Failed to fetch latest versionsGz: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Last-Modified", versionsGz[0].lastModified.Format(http.TimeFormat))
	w.Header().Set("ETag", versionsGz[0].etag)
	w.Header().Set("LatestReplicated-StorageServer", versionsGz[0].storageServer)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		w.Write(versionsGz[0].contents)
	}
}
