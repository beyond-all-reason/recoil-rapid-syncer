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
)

const versionsGzRepo = "byar"
const versionsGzFile = "/" + versionsGzRepo + "/versions.gz"

func (s *Server) sfFetchLatestSyncedVersionsGZ(ctx context.Context) ([]*replicatedFile, error) {
	return s.versionsGzCache.Get(ctx, func() ([]*replicatedFile, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		return s.fetchReplicatedFile(ctx, versionsGzFile)
	})
}

type byServerName []*replicatedFile

func (s byServerName) Len() int           { return len(s) }
func (s byServerName) Less(i, j int) bool { return s[i].storageServer < s[i].storageServer }
func (s byServerName) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *Server) HandleReplicationStatusVersionsGz(w http.ResponseWriter, r *http.Request) {
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
