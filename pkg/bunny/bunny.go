// SPDX-FileCopyrightText: 2022 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package bunny

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

type Client struct {
	accessKey string
	client    http.Client
}

func NewClient(accessKey string) *Client {
	return &Client{
		accessKey: accessKey,
		client: http.Client{
			Timeout: time.Second * 20,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxConnsPerHost:     70,
				MaxIdleConnsPerHost: 70,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
	}
}

const ClientUserAgent = "go-bunnyapi-client 1.0"

var (
	serverRE        = regexp.MustCompile(`^BunnyCDN-([A-Z]+\d*)-(\d+)$`)
	storageServerRE = regexp.MustCompile(`^([A-Z]+)-(\d+)$`)
)

func (c *Client) EdgeServersIP(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.bunny.net/system/edgeserverlist", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ClientUserAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := c.client.Do(req)
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

type Region struct {
	Id               int
	Name             string
	PricePerGigabyte float64
	RegionCode       string
	ContinentCode    string
	CountryCode      string
	Latitude         float64
	Longitude        float64
}

func (c *Client) Regions(ctx context.Context) ([]Region, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.bunny.net/region", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ClientUserAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	var regions []Region
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&regions)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json for edgeserverlist: %v", err)
	}
	return regions, nil
}

func ServerRegionCode(h *http.Header) (string, error) {
	m := serverRE.FindStringSubmatch(h.Get("Server"))
	if m == nil {
		return "", fmt.Errorf("failed to parse server name, got %s", h.Get("Server"))
	}
	return m[1], nil
}

func StorageServerRegionCode(h *http.Header) (string, error) {
	m := storageServerRE.FindStringSubmatch(h.Get("CDN-StorageServer"))
	if m == nil {
		return "", fmt.Errorf("failed to parse server name, got %s", h.Get("CDN-StorageServer"))
	}
	return m[1], nil
}

func angleToKm(angle s1.Angle) float64 {
	earthRadiusKm := 6371.0
	return angle.Radians() * earthRadiusKm
}

func RegionsDistanceKm(a, b *Region) float64 {
	aLL := s2.LatLngFromDegrees(a.Latitude, a.Longitude)
	bAll := s2.LatLngFromDegrees(b.Latitude, b.Longitude)
	return angleToKm(aLL.Distance(bAll))
}
