// SPDX-FileCopyrightText: 2022 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package bunny

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
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
	serverRE        = regexp.MustCompile(`^BunnyCDN-([A-Z]+\d*)1-(\d+)$`)
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
		return nil, fmt.Errorf("failed to decode json for edgeserverlist: %w", err)
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
		return nil, fmt.Errorf("failed to decode json for /region: %w", err)
	}
	return regions, nil
}

type StorageZone struct {
	Id                          int
	UserId                      string
	Name                        string
	Password                    string
	DateModified                string
	Deleted                     bool
	StorageUsed                 int64
	FilesStored                 int64
	Region                      string
	ReplicationRegions          []string
	ReadOnlyPassword            string
	Rewrite404To200             bool
	Custom404FilePath           string
	StorageHostname             string
	ZoneTier                    int
	ReplicationChangeInProgress bool
	PriceOverride               float64
	Discount                    int
}

func (c *Client) StorageZones(ctx context.Context) ([]StorageZone, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.bunny.net/storagezone?perPage=1000", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ClientUserAgent)
	req.Header.Set("AccessKey", c.accessKey)
	req.Header.Set("Accept", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	var storageZones []StorageZone
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&storageZones)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json for /storagezone: %w", err)
	}
	return storageZones, nil
}

func (c *Client) StorageZoneByName(ctx context.Context, name string) (*StorageZone, error) {
	zones, err := c.StorageZones(ctx)
	if err != nil {
		return nil, err
	}
	for _, zone := range zones {
		if zone.Name == name {
			return &zone, nil
		}
	}
	return nil, fmt.Errorf("storage zone %s not found", name)
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

type StorageZoneClient struct {
	client          *Client
	zoneName        string
	storageEndpoint string
	accessKey       string
}

func NewStorageZoneClient(zoneName, storageEndpoint, accessKey string) *StorageZoneClient {
	return &StorageZoneClient{
		client:          NewClient("invalid"),
		zoneName:        zoneName,
		storageEndpoint: storageEndpoint,
		accessKey:       accessKey,
	}
}

func (c *Client) NewStorageZoneClient(ctx context.Context, zoneName string) (*StorageZoneClient, error) {
	sz := &StorageZoneClient{
		client:   c,
		zoneName: zoneName,
	}
	zone, err := c.StorageZoneByName(ctx, zoneName)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage zone: %w", err)
	}
	sz.storageEndpoint = zone.StorageHostname
	sz.accessKey = zone.Password
	return sz, nil
}

func (sz *StorageZoneClient) getFileUrl(filePath string) string {
	fileName := path.Base(filePath)
	dir := path.Dir(filePath)
	apiUrl := "https://" + sz.storageEndpoint + "/" + sz.zoneName + "/"
	if dir != "." && dir != "/" {
		apiUrl += url.PathEscape(dir) + "/"
	}
	apiUrl += url.PathEscape(fileName)
	return apiUrl
}

func sha256AndLengthFromReader(r interface {
	io.Seeker
	io.Reader
}) ([]byte, int64, error) {
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, err
	}
	h := sha256.New()
	length, err := io.Copy(h, r)
	if err != nil {
		return nil, 0, err
	}
	if _, err := r.Seek(pos, io.SeekStart); err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), length, nil
}

func (sz *StorageZoneClient) Upload(ctx context.Context, filePath string, contents io.Reader) error {
	var err error
	var contentsHash []byte = nil
	var contentsLength int64 = 0
	if v, ok := contents.(interface {
		io.Seeker
		io.Reader
	}); ok {
		contentsHash, contentsLength, err = sha256AndLengthFromReader(v)
		if err != nil {
			return err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, sz.getFileUrl(filePath), contents)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", ClientUserAgent)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("AccessKey", sz.accessKey)
	if contentsHash != nil {
		req.Header.Set("Checksum", strings.ToUpper(hex.EncodeToString(contentsHash[:])))
		req.Header.Set("Content-Length", fmt.Sprintf("%d", contentsLength))
	}
	resp, err := sz.client.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("http put request failed with code: %d", resp.StatusCode)
	}
	return nil
}

func (sz *StorageZoneClient) Delete(ctx context.Context, filePath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, sz.getFileUrl(filePath), nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", ClientUserAgent)
	req.Header.Set("AccessKey", sz.accessKey)
	resp, err := sz.client.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http delete request failed with code: %d", resp.StatusCode)
	}
	return nil
}

func (sz *StorageZoneClient) Download(ctx context.Context, filePath string) (io.ReadCloser, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sz.getFileUrl(filePath), nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", ClientUserAgent)
	req.Header.Set("Accept", "*/*")
	resp, err := sz.client.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	return resp.Body, http.StatusOK, nil
}

func (sz *StorageZoneClient) List(ctx context.Context, dirPath string) ([]string, error) {
	apiUrl := "https://" + sz.storageEndpoint + "/" + sz.zoneName + "/" + url.PathEscape(dirPath) + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", ClientUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("AccessKey", sz.accessKey)
	resp, err := sz.client.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}

	var entries []struct {
		ObjectName string
	}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&entries)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json: %v", err)
	}
	files := make([]string, len(entries))
	for i, entry := range entries {
		files[i] = entry.ObjectName
	}
	return files, nil
}
