// SPDX-FileCopyrightText: 2022,2026 Marek Rusinowski
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"path"
	"regexp"
	"sync"
	"time"

	"github.com/beyond-all-reason/recoil-rapid-syncer/pkg/bunny"
)

type archive struct {
	tag, hash string
}

type entry struct {
	name, hash  string
	crc32, size uint32
}

type RapidSyncer struct {
	client http.Client
	dst    bunny.StorageZoneOperations
}

var (
	poolFileRegex = regexp.MustCompile("^[0-9a-f]{30}\\.gz$")
)

func NewRapidSyncer(dst bunny.StorageZoneOperations) *RapidSyncer {
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
		dst: dst,
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

func parseVersions(r io.Reader) ([]archive, error) {
	var archives []archive
	versionsReader := csv.NewReader(r)
	for {
		record, err := versionsReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("error when reading: %v", err)
		} else if len(record) < 4 {
			return nil, fmt.Errorf("invalid versions line")
		}
		archives = append(archives, archive{tag: record[0], hash: record[1]})
	}
	return archives, nil
}

func (rs *RapidSyncer) fetchSourceVersions(ctx context.Context, srcRepo string) ([]archive, []byte, error) {
	var archives []archive
	headers := map[string]string{"Cache-Control": "no-cache"}
	buf, code, err := rs.fetchGzipFile(ctx, srcRepo+"versions.gz", headers, func(r io.Reader) error {
		var perr error
		archives, perr = parseVersions(r)
		return perr
	})
	if code == http.StatusNotFound {
		return archives, []byte{}, nil
	}
	return archives, buf, err
}

func (rs *RapidSyncer) fetchDestVersions(ctx context.Context, dstPrefix string) ([]archive, error) {
	rc, status, err := rs.dst.Download(ctx, path.Join(dstPrefix, "versions.gz"))
	if status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	gzReader, err := gzip.NewReader(rc)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %v", err)
	}
	return parseVersions(gzReader)
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

func (rs *RapidSyncer) uploadFile(ctx context.Context, dstPrefix, filePath string, contents []byte) error {
	return rs.dst.Upload(ctx, path.Join(dstPrefix, filePath), bytes.NewReader(contents))
}

func (rs *RapidSyncer) getAvailableFilesWithPrefix(ctx context.Context, dstPrefix string, prefix byte) ([]string, error) {
	names, err := rs.dst.List(ctx, path.Join(dstPrefix, fmt.Sprintf("pool/%02x", prefix)))
	if err != nil {
		return nil, err
	}
	var files []string
	for _, name := range names {
		if !poolFileRegex.MatchString(name) {
			return nil, fmt.Errorf("one of the files in pool doesn't conform to the name: %s", name)
		}
		files = append(files, fmt.Sprintf("%02x%s", prefix, name[0:30]))
	}
	return files, nil
}

func (rs *RapidSyncer) getAvailableFiles(ctx context.Context, dstPrefix string) (map[string]struct{}, error) {
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
				files, err := rs.getAvailableFilesWithPrefix(subCtx, dstPrefix, prefix)
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
func (rs *RapidSyncer) compareVersions(ctx context.Context, srcRepo string, dstPrefix string) (bool, []string, []byte, error) {
	var srcArchives, destArchives []archive
	var srcVersions []byte
	var srcErr, destErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		srcArchives, srcVersions, srcErr = rs.fetchSourceVersions(ctx, srcRepo)
		wg.Done()
	}()
	go func() {
		destArchives, destErr = rs.fetchDestVersions(ctx, dstPrefix)
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

func (rs *RapidSyncer) syncMissingArchives(ctx context.Context, srcRepo string, dstPrefix string, archives []string) error {
	subCtx, cancel := context.WithCancel(ctx)
	errorCh := make(chan error, 1)

	gotFiles := make(chan struct{})
	var availFiles map[string]struct{}
	var availFiles_mu sync.Mutex

	go func() {
		var err error
		availFiles, err = rs.getAvailableFiles(subCtx, dstPrefix)
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
				err := rs.uploadFile(ctx, dstPrefix, u.path, u.contents)
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

func (rs *RapidSyncer) Sync(ctx context.Context, srcRepo string, dstPrefix string) (int, error) {
	same, missingArchives, versionsBuf, err := rs.compareVersions(ctx, srcRepo, dstPrefix)
	if err != nil {
		return 0, fmt.Errorf("compute missing archives: %v", err)
	} else if same {
		return 0, nil
	}
	if err = rs.syncMissingArchives(ctx, srcRepo, dstPrefix, missingArchives); err != nil {
		return 0, fmt.Errorf("syncing failed: %v", err)
	}
	if err = rs.uploadFile(ctx, dstPrefix, "versions.gz", versionsBuf); err != nil {
		return 0, fmt.Errorf("failed upload versions.gz: %v", err)
	}
	return len(missingArchives), nil
}
