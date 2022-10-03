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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"golang.org/x/sync/singleflight"
)

var (
	bunnyServerRE        = regexp.MustCompile(`^BunnyCDN-([A-Z]+\d*)-(\d+)$`)
	bunnyStorageServerRE = regexp.MustCompile(`^([A-Z]+)-(\d+)$`)
)

type Server struct {
	client                 http.Client
	versionsGzUrl          string
	maxRegionDistanceKm    float64
	expectedStorageServers []string
	serversMap             ServersMap
	serversMapMu           sync.RWMutex
	serversMapLastUpdate   time.Time
	gcsCacheBucket         string
	gcsClient              *storage.Client
	serversMapRefreshSFG   singleflight.Group
	triedFromGCS           bool
	versionsGzSFG          singleflight.Group
}

func (s *Server) getBunnyEdgeServers(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.bunny.net/system/edgeserverlist", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "spring-rapid-syncer/latestreplicated 1.0")
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	var servers []string
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&servers)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json for edgeserverlist: %v", err)
	}
	return servers, nil
}

type BunnyRegion struct {
	Id               int
	Name             string
	PricePerGigabyte float64
	RegionCode       string
	ContinentCode    string
	CountryCode      string
	Latitude         float64
	Longitude        float64
}

func (s *Server) getBunnyRegions(ctx context.Context) ([]BunnyRegion, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.bunny.net/region", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "spring-rapid-syncer/latestreplicated 1.0")
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	var regions []BunnyRegion
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&regions)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json for edgeserverlist: %v", err)
	}
	return regions, nil
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
	req.Header.Set("User-Agent", "spring-rapid-syncer/latestreplicated 1.0")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := client.Do(req)
	if err != nil {
		return BunnyEdgeServer{IP: ip, Error: err}
	}
	defer resp.Body.Close()
	if resp.Header.Get("CDN-Cache") != "BYPASS" {
		return BunnyEdgeServer{IP: ip, Error: fmt.Errorf("cache must be bypasssed, got %s", resp.Header.Get("CDN-Cache"))}
	}
	serverMatch := bunnyServerRE.FindStringSubmatch(resp.Header.Get("Server"))
	if serverMatch == nil {
		return BunnyEdgeServer{IP: ip, Error: fmt.Errorf("failed to parse server name, got %s", resp.Header.Get("Server"))}
	}
	storageServerMatch := bunnyStorageServerRE.FindStringSubmatch(resp.Header.Get("CDN-StorageServer"))
	if storageServerMatch == nil {
		return BunnyEdgeServer{IP: ip, Error: fmt.Errorf("failed to parse server name, got %s", resp.Header.Get("CDN-StorageServer"))}
	}
	return BunnyEdgeServer{
		IP:                  ip,
		ServerRegion:        serverMatch[1],
		StorageServerRegion: storageServerMatch[1],
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

func angleToKm(angle s1.Angle) float64 {
	earthRadiusKm := 6371.0
	return angle.Radians() * earthRadiusKm
}

func regionsDistanceKm(a, b *BunnyRegion) float64 {
	aLL := s2.LatLngFromDegrees(a.Latitude, a.Longitude)
	bAll := s2.LatLngFromDegrees(b.Latitude, b.Longitude)
	return angleToKm(aLL.Distance(bAll))
}

type ServersMap map[string][]string

func (s *Server) buildEdgeServersMap(ctx context.Context) (ServersMap, error) {
	regions, err := s.getBunnyRegions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get bunny regions: %v", err)
	}
	regByCode := make(map[string]*BunnyRegion)
	for i, reg := range regions {
		regByCode[reg.RegionCode] = &regions[i]
	}

	serverIPs, err := s.getBunnyEdgeServers(ctx)
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
			log.Printf("WARN: Failed to find region for edge region %s", serv.ServerRegion)
			continue
		}
		storageReg, ok := regByCode[serv.StorageServerRegion]
		if !ok {
			return nil, fmt.Errorf("failed to find region for storage region %s", serv.StorageServerRegion)
		}
		if regionsDistanceKm(edgeReg, storageReg) <= s.maxRegionDistanceKm {
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

func (s *Server) loadCachedServersMap(ctx context.Context) ServersMap {
	s.serversMapMu.RLock()
	serversMap := s.serversMap
	serversMapLastUpdate := s.serversMapLastUpdate
	triedFromGCS := s.triedFromGCS
	s.serversMapMu.RUnlock()

	if serversMap != nil && serversMapLastUpdate.After(time.Now().Add(-24*time.Hour)) {
		return serversMap
	} else if serversMap == nil && s.gcsClient != nil && !triedFromGCS {
		s.serversMapMu.Lock()
		defer s.serversMapMu.Unlock()

		subCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		// In case we were fighting for lock
		if s.serversMap != nil || s.triedFromGCS {
			return s.serversMap
		}

		obj := s.gcsClient.Bucket(s.gcsCacheBucket).Object("serversMap.json")
		attrs, err := obj.Attrs(subCtx)
		if err != nil && err != storage.ErrObjectNotExist {
			log.Printf("WARN: getting serversMap.json attrs failed: %v", err)
			return nil
		}
		if err == storage.ErrObjectNotExist || attrs.Updated.Before(time.Now().Add(-24*time.Hour)) {
			s.triedFromGCS = true
			return nil
		}
		r, err := obj.NewReader(subCtx)
		if err != nil {
			log.Printf("WARN: creating serversMap.json reader: %v", err)
			return nil
		}
		sm := make(ServersMap)
		decoder := json.NewDecoder(r)
		err = decoder.Decode(&sm)
		if err != nil {
			log.Printf("WARN: failed to decode json for servers map: %v", err)
			return nil
		}
		log.Printf("INFO: loaded serversMap.json from GCS")
		s.serversMap = sm
		s.serversMapLastUpdate = attrs.Updated
		return sm
	}

	return nil
}

func (s *Server) cacheServersMap(ctx context.Context, sm ServersMap) {
	if s.gcsCacheBucket != "" {
		obj := s.gcsClient.Bucket(s.gcsCacheBucket).Object("serversMap.json")
		w := obj.NewWriter(ctx)
		enc := json.NewEncoder(w)
		if err := enc.Encode(sm); err != nil {
			log.Printf("WARN: failed to encode serversMap: %v", err)
		}
		if err := w.Close(); err != nil {
			log.Printf("WARN: failed to write serversMap to GCS: %v", err)
		} else {
			log.Printf("INFO: cached serversMap.json in GCS")
		}
	}

	s.serversMapMu.Lock()
	defer s.serversMapMu.Unlock()
	s.serversMap = sm
	s.serversMapLastUpdate = time.Now()
}

func (s *Server) getEdgeServersMap(ctx context.Context) (ServersMap, error) {
	if sm := s.loadCachedServersMap(ctx); sm != nil {
		return sm, nil
	}
	resCh := s.serversMapRefreshSFG.DoChan("", func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		sm, err := s.buildEdgeServersMap(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to refresh serversMap: %v", err)
		}
		s.cacheServersMap(ctx, sm)
		return sm, err
	})

	select {
	case res := <-resCh:
		return res.Val.(ServersMap), res.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type versionsGzFile struct {
	contents      []byte
	lastModified  time.Time
	etag          string
	storageServer string
}

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

	storageServerMatch := bunnyStorageServerRE.FindStringSubmatch(resp.Header.Get("CDN-StorageServer"))
	if storageServerMatch == nil {
		return nil, fmt.Errorf("failed to parse server name, got %s", resp.Header.Get("CDN-StorageServer"))
	}
	if storageServerMatch[1] != expectedSS {
		return nil, fmt.Errorf("got response from unexpected storage server from edge %s: %s, expected %s", ip, storageServerMatch[1], expectedSS)
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

func (s *Server) fetchLatestSyncedVersionsGZ(ctx context.Context, sm ServersMap) (*versionsGzFile, []*versionsGzFile, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorCh := make(chan error, len(sm))
	versionsGzCh := make(chan *versionsGzFile, len(sm))
	for ss, ips := range sm {
		ip := ips[rand.Intn(len(ips))]
		go func(ss string, ip string) {
			ver, err := s.fetchVersionsGZ(subCtx, ip, ss)
			if err != nil {
				errorCh <- err
			} else {
				versionsGzCh <- ver
			}
		}(ss, ip)
	}
	var allVersionsGz []*versionsGzFile
	minVersionsGzFile := &versionsGzFile{lastModified: time.Now()}
	for i := 0; i < len(sm); i++ {
		select {
		case err := <-errorCh:
			return nil, nil, err
		case ver := <-versionsGzCh:
			allVersionsGz = append(allVersionsGz, ver)
			if ver.lastModified.Before(minVersionsGzFile.lastModified) {
				minVersionsGzFile = ver
			}
		}
	}
	return minVersionsGzFile, allVersionsGz, nil
}

func (s *Server) sfFetchLatestSyncedVersionsGZ(ctx context.Context, sm ServersMap) (*versionsGzFile, []*versionsGzFile, error) {
	type Res struct {
		minV *versionsGzFile
		allV []*versionsGzFile
	}
	resCh := s.versionsGzSFG.DoChan("", func() (interface{}, error) {
		minV, allV, err := s.fetchLatestSyncedVersionsGZ(context.Background(), sm)
		return Res{minV, allV}, err
	})
	select {
	case res := <-resCh:
		return res.Val.(Res).minV, res.Val.(Res).allV, res.Err
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (s *Server) HandleServerMap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	serversMap, err := s.getEdgeServersMap(r.Context())
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

	serversMap, err := s.getEdgeServersMap(r.Context())
	if err != nil {
		log.Printf("Failed to get edge server map: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	versionsGz, _, err := s.sfFetchLatestSyncedVersionsGZ(r.Context(), serversMap)
	if err != nil {
		log.Printf("Failed to fetch lastes versionsGz: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Last-Modified", versionsGz.lastModified.Format(http.TimeFormat))
	w.Header().Set("ETag", versionsGz.etag)
	w.Header().Set("LastReplicated-StorageServer", versionsGz.storageServer)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		w.Write(versionsGz.contents)
	}
}

type byServerName []*versionsGzFile

func (s byServerName) Len() int           { return len(s) }
func (s byServerName) Less(i, j int) bool { return s[i].storageServer < s[i].storageServer }
func (s byServerName) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *Server) HandleLatestReplicated(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	serversMap, err := s.getEdgeServersMap(r.Context())
	if err != nil {
		log.Printf("Failed to get edge server map: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	_, allVersionsGz, err := s.sfFetchLatestSyncedVersionsGZ(r.Context(), serversMap)
	if err != nil {
		log.Printf("Failed to fetch lastes versionsGz: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var out strings.Builder
	sort.Sort(byServerName(allVersionsGz))
	for _, ver := range allVersionsGz {
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

	gcsCacheBucket := os.Getenv("GCS_CACHE_BUCKET")
	ctx := context.Background()
	var gcsClient *storage.Client
	if gcsCacheBucket != "" {
		gcsClient, err = storage.NewClient(ctx)
		if err != nil {
			log.Fatalf("Failed to create GCS client: %v", err)
		}
	}

	server := &Server{
		client: http.Client{
			Timeout: time.Second * 5,
		},
		versionsGzUrl:          versionsGzUrl,
		maxRegionDistanceKm:    maxRegionDistancFloat,
		expectedStorageServers: strings.Split(expectedStorageRegions, ","),
		gcsCacheBucket:         gcsCacheBucket,
		gcsClient:              gcsClient,
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
