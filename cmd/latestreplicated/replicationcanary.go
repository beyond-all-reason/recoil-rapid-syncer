package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/p2004a/spring-rapid-syncer/pkg/bunny"
	"golang.org/x/sync/errgroup"
)

type replicatedFile struct {
	contents      []byte
	lastModified  time.Time
	etag          string
	storageServer string
}

const canaryFileName = "replication-canary.txt"

func (s *Server) startCanaryUpdater(ctx context.Context) {
	ticker := time.NewTicker(s.refreshReplicationCanaryPeriod)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			c, cancel := context.WithTimeout(ctx, s.refreshReplicationCanaryPeriod/2)
			s.updateReplicationCanary(c)
			cancel()
		}
	}
}

func (s *Server) updateReplicationCanary(ctx context.Context) {
	contents := strings.NewReader(time.Now().UTC().Format(time.RFC3339))
	err := s.bunnyStorageZone.Upload(ctx, canaryFileName, contents)
	if err != nil {
		log.Printf("ERROR: Failed to upload replication canary: %v", err)
	}
}

func (s *Server) fetchReplicatedFileFromIP(ctx context.Context, ip, expectedSS string, filePath string) (*replicatedFile, error) {
	client := httpClientForAddr(ip+":443", time.Second*20)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseUrl+filePath, nil)
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("downloading %s failed: %w", filePath, err)
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

	return &replicatedFile{
		contents:      body,
		lastModified:  modified,
		etag:          resp.Header.Get("ETag"),
		storageServer: expectedSS,
	}, nil
}

func (s *Server) fetchReplicatedFile(ctx context.Context, filePath string) ([]*replicatedFile, error) {
	grp, subCtx := errgroup.WithContext(ctx)

	serversMap, err := s.sfFetchStorageEdgeMap(subCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to get edge server map: %w", err)
	}

	allVersionsGz := make([]*replicatedFile, len(serversMap))
	i := 0
	for storageServer, ips := range serversMap {
		ip := ips[rand.Intn(len(ips))]
		id := i
		ss := storageServer
		grp.Go(func() error {
			var err error
			allVersionsGz[id], err = s.fetchReplicatedFileFromIP(subCtx, ip, ss, filePath)
			return err
		})
		i++
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}
	return allVersionsGz, nil
}

func (s *Server) HandleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	canaryFiles, err := s.fetchReplicatedFile(r.Context(), "/"+canaryFileName)
	if err != nil {
		log.Printf("Failed to fetch lastes versionsGz: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var out strings.Builder
	sort.Sort(byServerName(canaryFiles))
	for _, ver := range canaryFiles {
		out.WriteString(fmt.Sprintf("%s: \n  etag: %s\n  last-modified: %s\n  contents: %s\n",
			ver.storageServer, ver.etag, ver.lastModified.Format(http.TimeFormat), string(ver.contents)))
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, out.String())
}
