// SPDX-FileCopyrightText: 2023 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/bunny"
	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/sfcache"
	"golang.org/x/sync/errgroup"
)

type ReplicatedFile struct {
	contents      []byte
	lastModified  time.Time
	etag          string
	storageServer string
}

type ReplicatedFetcher struct {
	bunny                  *bunny.Client
	storageEdgeMapCache    sfcache.Cache[StorageEdgeMap]
	maxRegionDistanceKm    float64
	expectedStorageRegions []string
	probeFileUrl           string
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

// StorageEdgeMap is a map of storage server region to list of edge servers.
type StorageEdgeMap map[string][]string

func (s *ReplicatedFetcher) fetchStorageEdgeMap(ctx context.Context) (StorageEdgeMap, error) {
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
	servers := resolveBunnyEdgeServers(ctx, serverIPs, s.probeFileUrl)

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

	for _, expectedSS := range s.expectedStorageRegions {
		if _, ok := sm[expectedSS]; !ok {
			return nil, fmt.Errorf("failed to find expected storage server %s", expectedSS)
		}
	}
	if len(sm) > len(s.expectedStorageRegions) {
		return nil, fmt.Errorf("there are more sotrage servers in map then expected")
	}

	return sm, nil
}

func (s *ReplicatedFetcher) sfFetchStorageEdgeMap(ctx context.Context) (StorageEdgeMap, error) {
	return s.storageEdgeMapCache.Get(ctx, func() (StorageEdgeMap, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.fetchStorageEdgeMap(ctx)
	})
}

func (s *ReplicatedFetcher) FetchReplicatedFile(ctx context.Context, url string) ([]*ReplicatedFile, error) {
	grp, subCtx := errgroup.WithContext(ctx)

	serversMap, err := s.sfFetchStorageEdgeMap(subCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to get edge server map: %w", err)
	}

	allVersionsGz := make([]*ReplicatedFile, len(serversMap))
	i := 0
	for storageServer, ips := range serversMap {
		ip := ips[rand.Intn(len(ips))]
		id := i
		ss := storageServer
		grp.Go(func() error {
			var err error
			allVersionsGz[id], err = fetchFileFromBunnyIP(subCtx, ip, url)
			if err == nil && allVersionsGz[id].storageServer != ss {
				err = fmt.Errorf("got response from unexpected storage server from edge %s: %s, expected %s", ip, allVersionsGz[id].storageServer, ss)
			}
			return err
		})
		i++
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}
	return allVersionsGz, nil
}

func (s *ReplicatedFetcher) HandleStorageEdgeMap(w http.ResponseWriter, r *http.Request) {
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
