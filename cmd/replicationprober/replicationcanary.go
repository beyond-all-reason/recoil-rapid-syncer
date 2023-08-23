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

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
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
		c, cancel := context.WithTimeout(ctx, s.refreshReplicationCanaryPeriod/2)
		start := time.Now()
		err := s.updateReplicationCanary(c)
		cancel()
		var points []*influxdb3.Point
		if err != nil {
			log.Printf("WARN: Failed to update replication canary: %v", err)
			points = append(points,
				influxdb3.NewPointWithMeasurement("replication_canary_update_result").
					AddField("total", 1).
					AddField("error", 1).
					AddField("ok", 0).
					SetTimestamp(time.Now()))
		} else {
			latency := time.Since(start)
			points = append(points,
				influxdb3.NewPointWithMeasurement("replication_canary_update_result").
					AddField("total", 1).
					AddField("error", 0).
					AddField("ok", 1).
					SetTimestamp(time.Now()),
				influxdb3.NewPointWithMeasurement("replication_canary_update_latency").
					AddField("latency", latency.Milliseconds()).
					SetTimestamp(time.Now()))
		}

		c, cancel = context.WithTimeout(ctx, s.refreshReplicationCanaryPeriod/3)
		if err := s.influxdbClient.WritePoints(c, points...); err != nil {
			log.Printf("WARN: Failed to report replication_canary_update_latency to influxdb: %v", err)
		}
		cancel()

		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) startReplicationStatusChecker(ctx context.Context) {
	ticker := time.NewTicker(s.checkReplicationStatusPeriod)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
		}

		c, cancel := context.WithTimeout(ctx, s.checkReplicationStatusPeriod/2)
		start := time.Now()
		rs, err := s.fetchReplicationStatus(c)
		cancel()
		var points []*influxdb3.Point
		if err != nil {
			log.Printf("WARN: Failed to fetch replication status: %v", err)
			points = append(points,
				influxdb3.NewPointWithMeasurement("replication_status_check_result").
					AddField("total", 1).
					AddField("error", 1).
					AddField("ok", 0).
					SetTimestamp(time.Now()))
		} else {
			latency := time.Since(start)

			measurementTime := time.Now()

			points = append(points,
				influxdb3.NewPointWithMeasurement("replication_status_check_result").
					AddField("total", 1).
					AddField("error", 0).
					AddField("ok", 1).
					SetTimestamp(measurementTime),
				influxdb3.NewPointWithMeasurement("replication_status_check_latency").
					AddField("latency", latency.Milliseconds()).
					SetTimestamp(measurementTime))

			for _, r := range rs {
				points = append(points,
					influxdb3.NewPointWithMeasurement("replication_status_state").
						AddTag("storage_server", r.StorageServer).
						AddField("replicated", r.Replicated.Unix()).
						AddField("created", r.Created.Unix()).
						AddField("unsynced_for", r.UnsyncedFor.Seconds()).
						SetTimestamp(measurementTime))
			}
		}

		c, cancel = context.WithTimeout(ctx, s.checkReplicationStatusPeriod/3)
		if err := s.influxdbClient.WritePoints(c, points...); err != nil {
			log.Printf("WARN: Failed to report replication_status_check_latency to influxdb: %v", err)
		}
		cancel()
	}
}

func (s *Server) updateReplicationCanary(ctx context.Context) error {
	canary := time.Now().UTC()
	contents := canary.Format(time.RFC3339)

	err := s.bunnyStorageZone.Upload(ctx, canaryFileName, strings.NewReader(contents))
	if err != nil {
		return fmt.Errorf("failed to upload replication canary: %w", err)
	}

	s.latestCanary.mu.Lock()
	s.latestCanary.contents = contents
	s.latestCanary.mu.Unlock()
	return nil
}

func (s *Server) fetchReplicatedFileFromIP(ctx context.Context, ip, expectedSS string, filePath string) (*replicatedFile, error) {
	client := httpClientForAddr(ip+":443", time.Second*20)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseUrl+filePath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)
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

type ReplicationStatus struct {
	StorageServer string
	Replicated    time.Time
	Created       time.Time
	UnsyncedFor   time.Duration
}

func (s *Server) fetchReplicationStatus(ctx context.Context) ([]ReplicationStatus, error) {
	canaryFiles, err := s.fetchReplicatedFile(ctx, "/"+canaryFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch lastes versionsGz: %w", err)
	}

	s.latestCanary.mu.Lock()
	localContents := s.latestCanary.contents
	s.latestCanary.mu.Unlock()
	localCreated, _ := time.Parse(time.RFC3339, localContents)

	rs := make([]ReplicationStatus, len(canaryFiles))
	sort.Sort(byServerName(canaryFiles))
	for i, ver := range canaryFiles {
		contents := string(ver.contents)
		created, err := time.Parse(time.RFC3339, contents)
		if err != nil {
			return nil, fmt.Errorf("failed to parse written time: %w", err)
		}
		var unsyncedFor time.Duration
		if localContents != "" &&
			created.Before(localCreated) {
			unsyncedFor = time.Since(created.Add(s.refreshReplicationCanaryPeriod))
		}
		rs[i] = ReplicationStatus{
			StorageServer: ver.storageServer,
			Replicated:    ver.lastModified,
			Created:       created,
			UnsyncedFor:   unsyncedFor,
		}
	}
	return rs, nil
}

func (s *Server) HandleReplicationStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Got %s, not GET request for URL: %v", r.Method, r.URL)
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	rs, err := s.fetchReplicationStatus(r.Context())
	if err != nil {
		log.Printf("Failed to fetch replication status: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	var out strings.Builder
	for _, ver := range rs {
		out.WriteString(fmt.Sprintf("%s: \n  created: %s\n  replicated: %s\n  unsynced for: %s\n",
			ver.StorageServer, ver.Created, ver.Replicated, ver.UnsyncedFor))
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, out.String())
}
