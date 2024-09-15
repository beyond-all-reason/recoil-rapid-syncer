// SPDX-FileCopyrightText: 2022 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

type archive struct {
	tag, hash string
}

type entry struct {
	name, hash  string
	crc32, size uint32
}

type RapidSyncer struct {
	client         http.Client
	bunnyAccessKey string
}

var (
	poolFileRegex = regexp.MustCompile("^[0-9a-f]{30}\\.gz$")
)

func NewRapidSyncer(bunnyAccessKey string) *RapidSyncer {
	return &RapidSyncer{
		client: http.Client{
			Timeout: time.Second * 10,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxConnsPerHost:     75,
				MaxIdleConnsPerHost: 75,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
			},
		},
		bunnyAccessKey: bunnyAccessKey,
	}
}

func (rs *RapidSyncer) fetchGzipFile(
	ctx context.Context, url string, headers map[string]string,
	handler func(io.Reader) error) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", "recoil-rapid-syncer 1.0")
	for header, value := range headers {
		req.Header.Set(header, value)
	}
	resp, err := rs.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}
	var buf bytes.Buffer
	respBody := io.TeeReader(resp.Body, &buf)
	gzReader, err := gzip.NewReader(respBody)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create gzip reader: %v", err)
	}
	if err := handler(gzReader); err != nil {
		return nil, 0, err
	}
	return buf.Bytes(), http.StatusOK, nil
}

func (rs *RapidSyncer) fetchVersions(ctx context.Context, repo string, authBunny bool) ([]archive, []byte, error) {
	var archives []archive
	headers := map[string]string{"Cache-Control": "no-cache"}
	if authBunny {
		headers["AccessKey"] = rs.bunnyAccessKey
	}
	buf, code, err := rs.fetchGzipFile(ctx, repo+"versions.gz", headers, func(r io.Reader) error {
		versionsReader := csv.NewReader(r)
		for {
			record, err := versionsReader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("error when reading: %v", err)
			} else if len(record) < 4 {
				return fmt.Errorf("invalid versions line")
			}
			archives = append(archives, archive{tag: record[0], hash: record[1]})
		}
		return nil
	})
	if code == http.StatusNotFound {
		return archives, []byte{}, nil
	}
	return archives, buf, err
}

func archiveFileFromHash(hash string) string {
	return "packages/" + hash + ".sdp"
}

func (rs *RapidSyncer) fetchArchive(ctx context.Context, repo, archiveHash string) ([]entry, []byte, error) {
	var entries []entry
	url := repo + archiveFileFromHash(archiveHash)
	buf, _, err := rs.fetchGzipFile(ctx, url, map[string]string{}, func(r io.Reader) error {
		sdpData, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		buf := bytes.NewBuffer(sdpData)
		hash := md5.New()
		for {
			lenB, err := buf.ReadByte()
			if err == io.EOF {
				break
			}
			l := int(lenB)
			recordSize := l + md5.Size + 4 + 4
			b := buf.Next(recordSize)
			if len(b) != recordSize {
				return fmt.Errorf("unexpected end in the sdp file")
			}
			entries = append(entries, entry{
				name:  string(b[0:l]),
				hash:  hex.EncodeToString(b[l : l+md5.Size]),
				crc32: binary.BigEndian.Uint32(b[l+md5.Size : l+md5.Size+4]),
				size:  binary.BigEndian.Uint32(b[l+md5.Size+4 : recordSize]),
			})
			filenameHash := md5.Sum(b[0:l])
			hash.Write(filenameHash[:])
			hash.Write(b[l : l+md5.Size])
		}
		if hex.EncodeToString(hash.Sum([]byte{})) != archiveHash {
			return fmt.Errorf("sdp content doesn't match hash")
		}
		return nil
	})
	return entries, buf, err
}

func poolFileFromHash(hash string) string {
	return "pool/" + hash[0:2] + "/" + hash[2:] + ".gz"
}

func (rs *RapidSyncer) fetchPoolEntry(ctx context.Context, repo, entryHash string) ([]byte, error) {
	url := repo + poolFileFromHash(entryHash)
	buf, _, err := rs.fetchGzipFile(ctx, url, map[string]string{}, func(r io.Reader) error {
		hash := md5.New()
		_, err := io.Copy(hash, r)
		if err != nil {
			return fmt.Errorf("error when reading: %v", err)
		}
		if hex.EncodeToString(hash.Sum([]byte{})) != entryHash {
			return fmt.Errorf("contents doesn't match expected hash")
		}
		return nil
	})
	return buf, err
}

func (rs *RapidSyncer) uploadFile(ctx context.Context, repo, path string, contents []byte) error {
	reqBody := bytes.NewBuffer(contents)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, repo+path, reqBody)
	if err != nil {
		return err
	}
	contentsHash := sha256.Sum256(contents)
	req.Header.Set("User-Agent", "recoil-rapid-syncer 1.0")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("AccessKey", rs.bunnyAccessKey)
	req.Header.Set("Checksum", strings.ToUpper(hex.EncodeToString(contentsHash[:])))
	resp, err := rs.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("http put request failed with code: %d", resp.StatusCode)
	}
	return nil
}

func (rs *RapidSyncer) getAvailableFilesWithPrefix(ctx context.Context, repo string, prefix byte) ([]string, error) {
	url := fmt.Sprintf("%spool/%02x/", repo, prefix)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "recoil-rapid-syncer 1.0")
	req.Header.Set("AccessKey", rs.bunnyAccessKey)
	resp, err := rs.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request failed with code: %d", resp.StatusCode)
	}

	type fileEntry struct {
		ObjectName string
	}
	var entries []fileEntry

	var buf bytes.Buffer
	respBody := io.TeeReader(resp.Body, &buf)

	decoder := json.NewDecoder(respBody)
	err = decoder.Decode(&entries)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json for %s: %v", url, err)
	}

	var files []string
	for _, entry := range entries {
		if !poolFileRegex.MatchString(entry.ObjectName) {
			return nil, fmt.Errorf("one of the files in pool doesn't conform to the name: %s", entry.ObjectName)
		}
		files = append(files, fmt.Sprintf("%02x%s", prefix, entry.ObjectName[0:30]))
	}

	return files, nil
}

func (rs *RapidSyncer) getAvailableFiles(ctx context.Context, repo string) (map[string]struct{}, error) {
	subCtx, cancel := context.WithCancel(ctx)
	type result struct {
		files []string
		err   error
	}
	results := make(chan result, 256)
	inputs := make(chan byte, 256)

	for i := 0; i < 50; i++ {
		go func() {
			for prefix := range inputs {
				files, err := rs.getAvailableFilesWithPrefix(subCtx, repo, prefix)
				results <- result{files, err}
			}
		}()
	}

	for i := 0; i < 256; i++ {
		inputs <- byte(i)
	}
	filesSet := make(map[string]struct{})
	var firstError error
	for i := 0; i < 256; i++ {
		r := <-results
		if firstError != nil {
			continue
		}
		if r.err != nil {
			cancel()
			firstError = r.err
			continue
		}
		for _, file := range r.files {
			filesSet[file] = struct{}{}
		}
	}
	cancel() // To remove warning
	return filesSet, firstError
}

// Computes difference between the source and destination repos.
// Returns:
//
//	bool - are the same (even when missing archives can be empty,
//	       tags can point at different hashes)
//	[]string - list of hashes of missing sdp archives
//	[]byte - source versions
//	error - if there was any error during computation
func (rs *RapidSyncer) compareVersions(ctx context.Context, srcRepo string, dstRepo string) (bool, []string, []byte, error) {
	var srcArchives, destArchives []archive
	var srcVersions []byte
	var srcErr, destErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		srcArchives, srcVersions, srcErr = rs.fetchVersions(ctx, srcRepo, false)
		wg.Done()
	}()
	go func() {
		destArchives, _, destErr = rs.fetchVersions(ctx, dstRepo, true)
		wg.Done()
	}()
	wg.Wait()
	if srcErr != nil {
		return false, nil, nil, fmt.Errorf("fetching source archive: %v", srcErr)
	}
	if destErr != nil {
		return false, nil, nil, fmt.Errorf("fetching dest archive: %v", destErr)
	}
	if len(srcArchives) == 0 {
		return false, nil, nil, fmt.Errorf("source archive is empty/not-found")
	}
	destTagsToHash := make(map[string]string, len(destArchives))
	presentArchivesSet := make(map[string]struct{}, len(destArchives))
	for _, a := range destArchives {
		presentArchivesSet[a.hash] = struct{}{}
		destTagsToHash[a.tag] = a.hash
	}

	areSame := true
	var missingArchives []string
	for _, a := range srcArchives {
		if _, ok := presentArchivesSet[a.hash]; !ok {
			presentArchivesSet[a.hash] = struct{}{}
			missingArchives = append(missingArchives, a.hash)
		}
		if hash, ok := destTagsToHash[a.tag]; !ok || hash != a.hash {
			areSame = false
		}
	}

	return areSame, missingArchives, srcVersions, nil
}

func (rs *RapidSyncer) syncMissingArchives(ctx context.Context, srcRepo string, dstRepo string, archives []string) error {
	subCtx, cancel := context.WithCancel(ctx)
	errorCh := make(chan error, 1)

	gotFiles := make(chan struct{})
	var availFiles map[string]struct{}
	var availFiles_mu sync.Mutex

	go func() {
		var err error
		availFiles, err = rs.getAvailableFiles(subCtx, dstRepo)
		if err != nil {
			select {
			case errorCh <- err:
			default:
			}
		}
		close(gotFiles)
	}()

	var resultWg sync.WaitGroup

	type upload struct {
		contents []byte
		path     string
	}
	uploadCh := make(chan upload, 25)
	var uploadWg sync.WaitGroup
	for i := 0; i < 65; i++ {
		resultWg.Add(1)
		go func() {
			defer resultWg.Done()
			for u := range uploadCh {
				err := rs.uploadFile(ctx, dstRepo, u.path, u.contents)
				if err != nil {
					select {
					case errorCh <- err:
					default:
					}
					return
				}
			}
		}()
	}

	go func() {
		resultWg.Wait()
		select {
		case errorCh <- nil:
		default:
		}
	}()

	poolFetchCh := make(chan string, 25)
	var poolFetchWg sync.WaitGroup
	for i := 0; i < 50; i++ {
		uploadWg.Add(1)
		go func() {
			defer uploadWg.Done()
			for hash := range poolFetchCh {
				content, err := rs.fetchPoolEntry(subCtx, srcRepo, hash)
				if err != nil {
					select {
					case errorCh <- err:
					default:
					}
					return
				}
				select {
				case <-subCtx.Done():
					return
				case uploadCh <- upload{content, poolFileFromHash(hash)}:
				}
			}
		}()
	}

	archivesCh := make(chan string, 50)
	numWorkers := 50
	if numWorkers > len(archives) {
		numWorkers = len(archives)
	}
	for i := 0; i < numWorkers; i++ {
		uploadWg.Add(1)
		poolFetchWg.Add(1)
		go func() {
			defer uploadWg.Done()
			defer poolFetchWg.Done()
			for a := range archivesCh {
				entries, archiveBuf, err := rs.fetchArchive(subCtx, srcRepo, a)
				<-gotFiles
				if err != nil {
					select {
					case errorCh <- err:
					default:
					}
					return
				}
				select {
				case <-subCtx.Done():
					return
				case uploadCh <- upload{archiveBuf, archiveFileFromHash(a)}:
				}
				availFiles_mu.Lock()
				for _, e := range entries {
					if _, ok := availFiles[e.hash]; !ok {
						availFiles[e.hash] = struct{}{}
						select {
						case <-subCtx.Done():
							availFiles_mu.Unlock()
							return
						case poolFetchCh <- e.hash:
						}
					}
				}
				availFiles_mu.Unlock()
			}
		}()
	}

	go func() {
		uploadWg.Wait()
		close(uploadCh)
	}()

	go func() {
		poolFetchWg.Wait()
		close(poolFetchCh)
	}()

	go func() {
		defer close(archivesCh)
		for _, a := range archives {
			select {
			case <-subCtx.Done():
				return
			case archivesCh <- a:
			}
		}
	}()

	err := <-errorCh
	cancel()
	return err
}

func (rs *RapidSyncer) Sync(ctx context.Context, srcRepo string, dstRepo string) (int, error) {
	same, missingArchives, versionsBuf, err := rs.compareVersions(ctx, srcRepo, dstRepo)
	if err != nil {
		return 0, fmt.Errorf("compute missing archives: %v", err)
	} else if same {
		return 0, nil
	}
	if err = rs.syncMissingArchives(ctx, srcRepo, dstRepo, missingArchives); err != nil {
		return 0, fmt.Errorf("syncing failed: %v", err)
	}
	if err = rs.uploadFile(ctx, dstRepo, "versions.gz", versionsBuf); err != nil {
		return 0, fmt.Errorf("failed upload versions.gz: %v", err)
	}
	return len(missingArchives), nil
}
