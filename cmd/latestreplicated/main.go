// SPDX-FileCopyrightText: 2022 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/p2004a/spring-rapid-syncer/pkg/bunny"
	"github.com/p2004a/spring-rapid-syncer/pkg/sfcache"
)

const UserAgent = "spring-rapid-syncer/latestreplicated 1.0"

type Server struct {
	http                           http.Client
	bunny                          *bunny.Client
	bunnyStorageZone               *bunny.StorageZoneClient
	baseUrl                        string
	maxRegionDistanceKm            float64
	expectedStorageRegions         []string
	storageEdgeMapCache            sfcache.Cache[StorageEdgeMap]
	versionsGzCache                sfcache.Cache[[]*replicatedFile]
	refreshReplicationCanaryPeriod time.Duration
}

func getExpectedStorageRegions(ctx context.Context, bunnyClient *bunny.Client, storageZone string) ([]string, error) {
	zone, err := bunnyClient.StorageZoneByName(ctx, storageZone)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage zones: %w", err)
	}
	regions := zone.ReplicationRegions[:]
	regions = append(regions, zone.Region)
	return regions, nil
}

func main() {
	bunnyAccessKey := getRequiredStrEnv("BUNNY_ACCESS_KEY")
	bunnyClient := bunny.NewClient(bunnyAccessKey)

	bunnyStorageZone := getRequiredStrEnv("BUNNY_STORAGE_ZONE")
	ctx := context.Background()
	bunnyStorageZoneClient, err := bunnyClient.NewStorageZoneClient(ctx, bunnyStorageZone)
	if err != nil {
		log.Fatalf("Failed to create Bunny storage zone client: %v", err)
	}

	expectedStorageRegions, err := getExpectedStorageRegions(ctx, bunnyClient, bunnyStorageZone)
	if err != nil {
		log.Fatalf("Failed to get expected storage regions: %v", err)
	}

	server := &Server{
		http: http.Client{
			Timeout: time.Second * 5,
		},
		bunny:                  bunnyClient,
		bunnyStorageZone:       bunnyStorageZoneClient,
		baseUrl:                getStrEnv("BASE_URL", "https://repos-cdn.beyondallreason.dev"),
		maxRegionDistanceKm:    getFloatEnv("MAX_REGION_DISTANCE_KM", 400),
		expectedStorageRegions: expectedStorageRegions,
		storageEdgeMapCache: sfcache.Cache[StorageEdgeMap]{
			Timeout: getDurationEnv("STORAGE_EDGE_MAP_CACHE_DURATION", time.Hour*24),
		},
		versionsGzCache: sfcache.Cache[[]*replicatedFile]{
			Timeout: getDurationEnv("VERSION_GZ_CACHE_DURATION", time.Second*10),
		},
		refreshReplicationCanaryPeriod: getDurationEnv("REFRESH_REPLICATION_CANARY_PERIOD", time.Minute*5),
	}

	http.HandleFunc("/storageedgemap", server.HandleStorageEdgeMap)
	http.HandleFunc("/replicationstatus_versionsgz", server.HandleReplicationStatusVersionsGz)
	http.HandleFunc("/replicationstatus", server.HandleReplicationStatus)

	go server.startCanaryUpdater(ctx)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("Defaulting to port %s", port)
	}
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
