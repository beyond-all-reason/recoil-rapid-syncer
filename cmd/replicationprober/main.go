// SPDX-FileCopyrightText: 2022 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/influxdb3"
	"github.com/caarlos0/env/v9"
	"github.com/p2004a/spring-rapid-syncer/pkg/bunny"
	"github.com/p2004a/spring-rapid-syncer/pkg/sfcache"
)

const UserAgent = "spring-rapid-syncer/prober 1.0"

type Canary struct {
	mu       sync.Mutex
	contents string
}

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
	checkReplicationStatusPeriod   time.Duration
	influxdbClient                 *influxdb3.Client
	latestCanary                   Canary
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

type config struct {
	Bunny struct {
		AccessKey   string
		StorageZone string
	} `envPrefix:"BUNNY_"`
	InfluxDb struct {
		Url      string
		Token    string
		Database string
	} `envPrefix:"INFLUXDB_"`
	BaseUrl                        string        `envDefault:"https://repos-cdn.beyondallreason.dev"`
	MaxRegionDistanceKm            float64       `envDefault:"400"`
	StorageEdgeMapCacheDuration    time.Duration `envDefault:"24h"`
	VersionGzCacheDuration         time.Duration `envDefault:"10s"`
	RefreshReplicationCanaryPeriod time.Duration `envDefault:"5m"`
	CheckReplicationStatusPeriod   time.Duration `envDefault:"2m"`
	CheckRedirectStatusPeriod      time.Duration `envDefault:"2m"`
	EnableStorageReplicationProber bool          `envDefault:"true"`
	EnableRedirectStatusProber     bool          `envDefault:"true"`
	Port                           string        `envDefault:"8080"`
}

func main() {
	cfg := config{}
	if err := env.ParseWithOptions(&cfg, env.Options{
		RequiredIfNoDef:       true,
		UseFieldNameByDefault: true,
	}); err != nil {
		log.Fatalf("%+v\n", err)

	}

	bunnyClient := bunny.NewClient(cfg.Bunny.AccessKey)

	ctx := context.Background()
	bunnyStorageZoneClient, err := bunnyClient.NewStorageZoneClient(ctx, cfg.Bunny.StorageZone)
	if err != nil {
		log.Fatalf("Failed to create Bunny storage zone client: %v", err)
	}

	expectedStorageRegions, err := getExpectedStorageRegions(ctx, bunnyClient, cfg.Bunny.StorageZone)
	if err != nil {
		log.Fatalf("Failed to get expected storage regions: %v", err)
	}

	influxdbClient, err := influxdb3.New(influxdb3.ClientConfig{
		Host:     cfg.InfluxDb.Url,
		Token:    cfg.InfluxDb.Token,
		Database: cfg.InfluxDb.Database,
	})
	if err != nil {
		log.Fatalf("Failed to create InfluxDB client: %v", err)
	}

	server := &Server{
		http: http.Client{
			Timeout: time.Second * 5,
		},
		bunny:                  bunnyClient,
		bunnyStorageZone:       bunnyStorageZoneClient,
		baseUrl:                cfg.BaseUrl,
		maxRegionDistanceKm:    cfg.MaxRegionDistanceKm,
		expectedStorageRegions: expectedStorageRegions,
		storageEdgeMapCache: sfcache.Cache[StorageEdgeMap]{
			Timeout: cfg.StorageEdgeMapCacheDuration,
		},
		versionsGzCache: sfcache.Cache[[]*replicatedFile]{
			Timeout: cfg.VersionGzCacheDuration,
		},
		refreshReplicationCanaryPeriod: cfg.RefreshReplicationCanaryPeriod,
		checkReplicationStatusPeriod:   cfg.CheckRedirectStatusPeriod,
		influxdbClient:                 influxdbClient,
	}

	http.HandleFunc("/storageedgemap", server.HandleStorageEdgeMap)
	http.HandleFunc("/replicationstatus_versionsgz", server.HandleReplicationStatusVersionsGz)
	http.HandleFunc("/replicationstatus", server.HandleReplicationStatus)

	if cfg.EnableStorageReplicationProber {
		go server.startCanaryUpdater(ctx)
		go server.startReplicationStatusChecker(ctx)
	}

	redirectProber := &RedirectProber{
		bunny:                   bunnyClient,
		influxdbClient:          influxdbClient,
		repo:                    versionsGzRepo,
		versionGzUrl:            server.baseUrl + versionsGzFile,
		probePeriod:             cfg.CheckRedirectStatusPeriod,
		edgeServerRefreshPeriod: time.Hour * 1,
	}
	if cfg.EnableRedirectStatusProber {
		go redirectProber.startProber(ctx)
	}

	if err := http.ListenAndServe(":"+cfg.Port, nil); err != nil {
		log.Fatal(err)
	}
}
