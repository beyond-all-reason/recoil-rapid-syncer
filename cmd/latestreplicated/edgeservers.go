package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/p2004a/spring-rapid-syncer/pkg/bunny"
)

const GCS_STORAGE_EDGE_FILE = "storageEdgeMap.json"

type BunnyEdgeServer struct {
	IP, ServerRegion, StorageServerRegion string
	Error                                 error
}

func resolveBunnyEdgeServer(ctx context.Context, ip, markerUrl string) BunnyEdgeServer {
	client := httpClientForAddr(ip+":443", time.Second*5)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, markerUrl, nil)
	if err != nil {
		return BunnyEdgeServer{IP: ip, Error: err}
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return BunnyEdgeServer{IP: ip, Error: err}
	}
	defer resp.Body.Close()
	if resp.Header.Get("CDN-Cache") != "BYPASS" {
		return BunnyEdgeServer{IP: ip, Error: fmt.Errorf("cache must be bypasssed, got %s", resp.Header.Get("CDN-Cache"))}
	}
	serverRegion, err := bunny.ServerRegionCode(&resp.Header)
	if err != nil {
		return BunnyEdgeServer{IP: ip, Error: err}
	}
	storageServerRegion, err := bunny.StorageServerRegionCode(&resp.Header)
	if err != nil {
		return BunnyEdgeServer{IP: ip, Error: err}
	}
	return BunnyEdgeServer{
		IP:                  ip,
		ServerRegion:        serverRegion,
		StorageServerRegion: storageServerRegion,
		Error:               nil,
	}
}

func resolveBunnyEdgeServers(ctx context.Context, servers []string, markerUrl string) []BunnyEdgeServer {
	var wg sync.WaitGroup
	edgeServers := make([]BunnyEdgeServer, len(servers))
	for i := 0; i < len(servers); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			edgeServers[i] = resolveBunnyEdgeServer(ctx, servers[i], markerUrl)
		}(i)
	}
	wg.Wait()
	return edgeServers
}

// StorageEdgeMap is a map of storage server region to list of edge servers.
type StorageEdgeMap map[string][]string

func (s *Server) buildStorageEdgeMap(ctx context.Context) (StorageEdgeMap, error) {
	regions, err := s.bunny.Regions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bunny regions: %w", err)
	}
	regByCode := make(map[string]*bunny.Region)
	for i, reg := range regions {
		regByCode[reg.RegionCode] = &regions[i]
	}

	serverIPs, err := s.bunny.EdgeServersIP(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch edge servers: %w", err)
	}
	servers := resolveBunnyEdgeServers(ctx, serverIPs, s.versionsGzUrl)

	sm := make(map[string][]string)
	for _, serv := range servers {
		if serv.Error != nil {
			continue
		}
		edgeReg, ok := regByCode[serv.ServerRegion]
		if !ok {
			log.Printf("WARN: Failed to find bunny region for edge region code %s", serv.ServerRegion)
			continue
		}
		storageReg, ok := regByCode[serv.StorageServerRegion]
		if !ok {
			log.Printf("WARN: Failed to find region for storage region %s", serv.StorageServerRegion)
			continue
		}
		if bunny.RegionsDistanceKm(edgeReg, storageReg) <= s.maxRegionDistanceKm {
			sm[serv.StorageServerRegion] = append(sm[serv.StorageServerRegion], serv.IP)
		}
	}

	for _, expectedSS := range s.expectedStorageServers {
		if _, ok := sm[expectedSS]; !ok {
			return nil, fmt.Errorf("failed to find expected storage server %s", expectedSS)
		}
	}
	if len(sm) > len(s.expectedStorageServers) {
		return nil, fmt.Errorf("there are more sotrage servers in map then expected")
	}

	return sm, nil
}

func (s *Server) fetchStorageEdgeMapFromGCS(ctx context.Context) (StorageEdgeMap, error) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	obj := s.gcsClient.Bucket(s.gcsCacheBucket).Object(GCS_STORAGE_EDGE_FILE)
	attrs, err := obj.Attrs(ctx)
	if err != nil && err != storage.ErrObjectNotExist {
		return nil, fmt.Errorf("getting %s attrs failed: %w", GCS_STORAGE_EDGE_FILE, err)
	}
	if err == storage.ErrObjectNotExist || attrs.Updated.Before(time.Now().Add(-s.storageEdgeMapCacheDuration)) {
		return nil, nil
	}
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating %s reader: %w", GCS_STORAGE_EDGE_FILE, err)
	}
	sm := make(StorageEdgeMap)
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&sm); err != nil {
		return nil, fmt.Errorf("failed to decode json for storage edge map: %w", err)
	}
	return sm, nil
}

func (s *Server) saveStorageEdgeMapToGCS(ctx context.Context, sm StorageEdgeMap) error {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	obj := s.gcsClient.Bucket(s.gcsCacheBucket).Object(GCS_STORAGE_EDGE_FILE)
	w := obj.NewWriter(ctx)
	enc := json.NewEncoder(w)
	if err := enc.Encode(sm); err != nil {
		return fmt.Errorf("failed to encode %s: %w", GCS_STORAGE_EDGE_FILE, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to write %s to GCS: %w", GCS_STORAGE_EDGE_FILE, err)
	}
	return nil
}

func (s *Server) fetchStorageEdgeMap(ctx context.Context) (StorageEdgeMap, error) {
	if s.gcsClient != nil {
		sm, err := s.fetchStorageEdgeMapFromGCS(ctx)
		if err != nil {
			log.Printf("WARN: fetching from GCS failed: %v", err)
		} else if sm != nil {
			log.Printf("INFO: loaded %s from GCS", GCS_STORAGE_EDGE_FILE)
			return sm, nil
		}
	}
	sm, err := s.buildStorageEdgeMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("build edge server map: %w", err)
	}
	if s.gcsClient != nil {
		if err := s.saveStorageEdgeMapToGCS(ctx, sm); err != nil {
			log.Printf("WARN: failed to save to GCS: %v", err)
		} else {
			log.Printf("INFO: saved %s to GCS", GCS_STORAGE_EDGE_FILE)
		}
	}
	return sm, nil
}

func (s *Server) sfFetchStorageEdgeMap(ctx context.Context) (StorageEdgeMap, error) {
	return s.storageEdgeMapCache.Get(ctx, func() (StorageEdgeMap, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.fetchStorageEdgeMap(ctx)
	})
}

func (s *Server) HandleStorageEdgeMap(w http.ResponseWriter, r *http.Request) {
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
	var out strings.Builder
	for region, servers := range serversMap {
		out.WriteString(fmt.Sprintf("%s %v", region, servers))
		out.WriteString("\n")
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, out.String())
}
