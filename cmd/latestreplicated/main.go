// SPDX-FileCopyrightText: 2022 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/p2004a/spring-rapid-syncer/pkg/bunny"
	"github.com/p2004a/spring-rapid-syncer/pkg/sfcache"
)

const UserAgent = "spring-rapid-syncer/latestreplicated 1.0"

type Server struct {
	http                        http.Client
	bunny                       *bunny.Client
	versionsGzUrl               string
	maxRegionDistanceKm         float64
	expectedStorageServers      []string
	gcsCacheBucket              string
	gcsClient                   *storage.Client
	storageEdgeMapCacheDuration time.Duration
	storageEdgeMapCache         sfcache.Cache[StorageEdgeMap]
	versionsGzCache             sfcache.Cache[[]*versionsGzFile]
}

func httpClientForAddr(addr string, timeout time.Duration) http.Client {
	return http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "tcp", addr)
			},
			ForceAttemptHTTP2: true,
		},
	}
}

func (s *Server) HandleLatestReplicated(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	serversMap, err := s.sfFetchStorageEdgeMap(r.Context())
	if err != nil {
		log.Printf("Failed to get storage edge server map: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	versionsGz, err := s.sfFetchLatestSyncedVersionsGZ(r.Context(), serversMap)
	if err != nil {
		log.Printf("Failed to fetch lastes versionsGz: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var out strings.Builder
	sort.Sort(byServerName(versionsGz))
	for _, ver := range versionsGz {
		out.WriteString(fmt.Sprintf("%s: %s (etag: %s)", ver.storageServer, ver.lastModified.Format(http.TimeFormat), ver.etag))
		out.WriteString("\n")
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, out.String())
}

func main() {
	expectedStorageRegions := os.Getenv("EXPECTED_STORAGE_REGIONS")
	if expectedStorageRegions == "" {
		log.Fatalf("Missing required env variable EXPECTED_STORAGE_REGIONS")
	}

	versionsGzUrl := os.Getenv("VERSIONS_GZ_URL")
	if versionsGzUrl == "" {
		versionsGzUrl = "https://repos-cdn.beyondallreason.dev/byar/versions.gz"
	}

	maxRegionDistance := os.Getenv("MAX_REGION_DISTANCE_KM")
	if maxRegionDistance == "" {
		maxRegionDistance = "400"
	}
	maxRegionDistancFloat, err := strconv.ParseFloat(maxRegionDistance, 64)
	if err != nil {
		log.Fatalf("Failed to parse MAX_REGION_DISTANCE_KM env as float: %v", err)
	}

	versionsGzCacheDurationStr := os.Getenv("VERSION_GZ_CACHE_DURATION")
	if versionsGzCacheDurationStr == "" {
		versionsGzCacheDurationStr = "10s"
	}
	versionsGzCacheDuration, err := time.ParseDuration(versionsGzCacheDurationStr)
	if err != nil {
		log.Fatalf("Failed to parse VERSION_GZ_CACHE_DURATION: %v", err)
	}

	storageEdgeMapCacheDurationStr := os.Getenv("STORAGE_EDGE_MAP_CACHE_DURATION")
	if storageEdgeMapCacheDurationStr == "" {
		storageEdgeMapCacheDurationStr = "24h"
	}
	storageEdgeMapCacheDuration, err := time.ParseDuration(storageEdgeMapCacheDurationStr)
	if err != nil {
		log.Fatalf("Failed to parse STORAGE_EDGE_MAP_CACHE_DURATION: %v", err)
	}
	storageEdgeMapCacheDurationLocal := storageEdgeMapCacheDuration

	gcsCacheBucket := os.Getenv("GCS_CACHE_BUCKET")
	ctx := context.Background()
	var gcsClient *storage.Client
	if gcsCacheBucket != "" {
		gcsClient, err = storage.NewClient(ctx)
		if err != nil {
			log.Fatalf("Failed to create GCS client: %v", err)
		}
		storageEdgeMapCacheDurationLocal /= 4
	}

	server := &Server{
		http: http.Client{
			Timeout: time.Second * 5,
		},
		bunny:                       bunny.NewClient(""),
		versionsGzUrl:               versionsGzUrl,
		maxRegionDistanceKm:         maxRegionDistancFloat,
		expectedStorageServers:      strings.Split(expectedStorageRegions, ","),
		gcsCacheBucket:              gcsCacheBucket,
		gcsClient:                   gcsClient,
		storageEdgeMapCacheDuration: storageEdgeMapCacheDuration,
		storageEdgeMapCache: sfcache.Cache[StorageEdgeMap]{
			Timeout: storageEdgeMapCacheDurationLocal,
		},
		versionsGzCache: sfcache.Cache[[]*versionsGzFile]{
			Timeout: versionsGzCacheDuration,
		},
	}

	http.HandleFunc("/storageedgemap", server.HandleStorageEdgeMap)
	http.HandleFunc("/latestreplicated", server.HandleLatestReplicated)
	http.HandleFunc("/versions.gz", server.HandleVersionsGz)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
