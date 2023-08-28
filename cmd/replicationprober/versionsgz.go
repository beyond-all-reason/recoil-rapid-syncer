package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/p2004a/spring-rapid-syncer/pkg/sfcache"
)

type VersionGzFetcher struct {
	replicatedFetcher *ReplicatedFetcher
	versionGzUrl      string
	versionsGzCache   sfcache.Cache[[]*ReplicatedFile]
}

func (s *VersionGzFetcher) sfFetchLatestSyncedVersionsGZ(ctx context.Context) ([]*ReplicatedFile, error) {
	return s.versionsGzCache.Get(ctx, func() ([]*ReplicatedFile, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		return s.replicatedFetcher.FetchReplicatedFile(ctx, s.versionGzUrl)
	})
}

type byServerName []*ReplicatedFile

func (s byServerName) Len() int           { return len(s) }
func (s byServerName) Less(i, j int) bool { return s[i].storageServer < s[j].storageServer }
func (s byServerName) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *VersionGzFetcher) HandleReplicationStatusVersionsGz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	versionsGz, err := s.sfFetchLatestSyncedVersionsGZ(r.Context())
	if err != nil {
		log.Printf("Failed to fetch lastes versionsGz: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var out strings.Builder
	sort.Sort(byServerName(versionsGz))
	for _, ver := range versionsGz {
		out.WriteString(fmt.Sprintf("%s: %s (etag: %s)",
			ver.storageServer, ver.lastModified.Format(http.TimeFormat), ver.etag))
		out.WriteString("\n")
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, out.String())
}
