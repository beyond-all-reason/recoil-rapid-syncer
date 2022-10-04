// SPDX-FileCopyrightText: 2022 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/p2004a/spring-rapid-syncer/pkg/bunny"
	"github.com/p2004a/spring-rapid-syncer/pkg/sfcache"
	"golang.org/x/sync/errgroup"
)

const UserAgent = "spring-rapid-syncer/latestreplicated 1.0"

type Server struct {
	http                   http.Client
	bunny                  *bunny.Client
	versionsGzUrl          string
	maxRegionDistanceKm    float64
	expectedStorageServers []string
	gcsCacheBucket         string
	gcsClient              *storage.Client
	serverMapCacheDuration time.Duration
	serversMapCache        sfcache.Cache[ServersMap]
	versionsGzCache        sfcache.Cache[[]*versionsGzFile]
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

type ServersMap map[string][]string

func (s *Server) buildEdgeServersMap(ctx context.Context) (ServersMap, error) {
	regions, err := s.bunny.Regions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bunny regions: %v", err)
	}
	regByCode := make(map[string]*bunny.Region)
	for i, reg := range regions {
		regByCode[reg.RegionCode] = &regions[i]
	}

	serverIPs, err := s.bunny.EdgeServersIP(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch edge servers: %v", err)
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
			return nil, fmt.Errorf("failed to find region for storage region %s", serv.StorageServerRegion)
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

func (s *Server) fetchEdgeServersMapFromGCS(ctx context.Context) (ServersMap, error) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	obj := s.gcsClient.Bucket(s.gcsCacheBucket).Object("serversMap.json")
	attrs, err := obj.Attrs(ctx)
	if err != nil && err != storage.ErrObjectNotExist {
		return nil, fmt.Errorf("getting serversMap.json attrs failed: %v", err)
	}
	if err == storage.ErrObjectNotExist || attrs.Updated.Before(time.Now().Add(-s.serverMapCacheDuration)) {
		return nil, nil
	}
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating serversMap.json reader: %v", err)
	}
	sm := make(ServersMap)
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&sm); err != nil {
		return nil, fmt.Errorf("failed to decode json for servers map: %v", err)
	}
	return sm, nil
}

func (s *Server) saveEdgeServersMapToGCS(ctx context.Context, sm ServersMap) error {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	obj := s.gcsClient.Bucket(s.gcsCacheBucket).Object("serversMap.json")
	w := obj.NewWriter(ctx)
	enc := json.NewEncoder(w)
	if err := enc.Encode(sm); err != nil {
		return fmt.Errorf("failed to encode serversMap: %v", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to write serversMap to GCS: %v", err)
	}
	return nil
}

func (s *Server) fetchEdgeServersMap(ctx context.Context) (ServersMap, error) {
	if s.gcsClient != nil {
		sm, err := s.fetchEdgeServersMapFromGCS(ctx)
		if err != nil {
			log.Printf("WARN: fetching from GCS failed: %v", err)
		} else if sm != nil {
			log.Printf("INFO: loaded serversMap.json from GCS")
			return sm, nil
		}
	}
	sm, err := s.buildEdgeServersMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("build edge server map: %v", err)
	}
	if s.gcsClient != nil {
		if err := s.saveEdgeServersMapToGCS(ctx, sm); err != nil {
			log.Printf("WARN: failed to save to GCS: %v", err)
		} else {
			log.Printf("INFO: saved serversMap.json to GCS")
		}
	}
	return sm, nil
}

func (s *Server) sfFetchEdgeServersMap(ctx context.Context) (ServersMap, error) {
	return s.serversMapCache.Get(ctx, func() (ServersMap, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.fetchEdgeServersMap(ctx)
	})
}

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
		return nil, fmt.Errorf("request to ip %s failed with: %v", ip, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("downloading versions.gz failed: %v", err)
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
		return nil, fmt.Errorf("failed to parse Last-Modified: %v", err)
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

func (s *Server) HandleServerMap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	serversMap, err := s.sfFetchEdgeServersMap(r.Context())
	if err != nil {
		log.Printf("Failed to get edge server map: %v", err)
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
		log.Printf("Failed to fetch lastes versionsGz: %v", err)
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

func (s *Server) HandleLatestReplicated(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
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
		versionsGzUrl = "https://bar-rapid.p2004a.com/byar/versions.gz"
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

	serverMapCacheDurationStr := os.Getenv("SERVER_MAP_CACHE_DURATION")
	if serverMapCacheDurationStr == "" {
		serverMapCacheDurationStr = "24h"
	}
	serverMapCacheDuration, err := time.ParseDuration(serverMapCacheDurationStr)
	if err != nil {
		log.Fatalf("Failed to parse SERVER_MAP_CACHE_DURATION: %v", err)
	}
	serverMapCacheDurationLocal := serverMapCacheDuration

	gcsCacheBucket := os.Getenv("GCS_CACHE_BUCKET")
	ctx := context.Background()
	var gcsClient *storage.Client
	if gcsCacheBucket != "" {
		gcsClient, err = storage.NewClient(ctx)
		if err != nil {
			log.Fatalf("Failed to create GCS client: %v", err)
		}
		serverMapCacheDurationLocal /= 4
	}

	server := &Server{
		http: http.Client{
			Timeout: time.Second * 5,
		},
		bunny:                  bunny.NewClient(""),
		versionsGzUrl:          versionsGzUrl,
		maxRegionDistanceKm:    maxRegionDistancFloat,
		expectedStorageServers: strings.Split(expectedStorageRegions, ","),
		gcsCacheBucket:         gcsCacheBucket,
		gcsClient:              gcsClient,
		serverMapCacheDuration: serverMapCacheDuration,
		serversMapCache: sfcache.Cache[ServersMap]{
			Timeout: serverMapCacheDurationLocal,
		},
		versionsGzCache: sfcache.Cache[[]*versionsGzFile]{
			Timeout: versionsGzCacheDuration,
		},
	}

	http.HandleFunc("/servermap", server.HandleServerMap)
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
