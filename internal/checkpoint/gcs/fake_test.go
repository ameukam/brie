// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

const fakeToken = "ya29.SENTINEL-do-not-log"

// fakeGCS is an in-process stand-in for the Cloud Storage JSON API with
// fault injection for the G3 storage cases. Each fault counter is consumed
// by one matching request.
type fakeGCS struct {
	t      *testing.T
	server *httptest.Server

	mu         sync.Mutex
	objects    map[string]fakeObject
	generation int64
	log        []string

	uploadStatuses  []int // statuses returned before reading an upload
	statStatuses    []int // statuses returned before a metadata read
	loseUploadReply int   // store the object, then reply 503
	cutUploads      int   // read part of the body, then drop the connection
	corruptStore    int   // store flipped bytes; metadata describes them
	mangleTransit   int   // flip bytes before the server's checksum check
	staleReadBack   int   // report a different checksum on the next metadata read
	mediaStatuses   []int // statuses returned before an object download
	mangleMedia     int   // flip a byte of the next object download
	uploadDelay     time.Duration
}

type fakeObject struct {
	generation int64
	data       []byte
	crc32c     string
}

func newFakeGCS(t *testing.T) *fakeGCS {
	t.Helper()
	fake := &fakeGCS{t: t, objects: map[string]fakeObject{}, generation: 1700000000000000}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload/storage/v1/b/{bucket}/o", fake.upload)
	mux.HandleFunc("GET /storage/v1/b/{bucket}/o/{object...}", fake.get)
	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeGCS) store(t *testing.T, config Config) *Store {
	t.Helper()
	if config.Tokens == nil {
		config.Tokens = staticTokens(t)
	}
	config.Endpoint = f.server.URL
	if config.InitialBackoff == 0 {
		config.InitialBackoff, config.MaxBackoff = time.Millisecond, 2*time.Millisecond
	}
	store, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type tokenFunc func() (AccessToken, error)

func staticTokens(t *testing.T) TokenSource {
	token, err := NewAccessToken(fakeToken, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return tokenFunc(func() (AccessToken, error) { return token, nil })
}

func (f tokenFunc) Token(context.Context) (AccessToken, error) { return f() }

func pop(values *[]int) int {
	if len(*values) == 0 {
		return 0
	}
	value := (*values)[0]
	*values = (*values)[1:]
	return value
}

func checksumsOf(data []byte) (string, string) {
	crc := encodeCRC32C(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli)))
	sum := md5.Sum(data)
	return crc, base64.StdEncoding.EncodeToString(sum[:])
}

func (f *fakeGCS) authorized(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Authorization") != "Bearer "+fakeToken {
		writeError(w, http.StatusUnauthorized, "required")
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, status int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"error":{"code":`+strconv.Itoa(status)+`,"message":"echo `+fakeToken+`","errors":[{"reason":"`+reason+`"}]}}`)
}

func (f *fakeGCS) metadata(bucket, name string, object fakeObject) map[string]string {
	_, md5Hash := checksumsOf(object.data)
	return map[string]string{
		"kind": "storage#object", "bucket": bucket, "name": name,
		"generation": strconv.FormatInt(object.generation, 10),
		"size":       strconv.Itoa(len(object.data)), "crc32c": object.crc32c, "md5Hash": md5Hash,
	}
}

func (f *fakeGCS) upload(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	bucket := r.PathValue("bucket")
	query := r.URL.Query()
	if query.Get("uploadType") != "multipart" || query.Get("ifGenerationMatch") != "0" {
		f.t.Errorf("upload without immutability precondition: %s", r.URL.RawQuery)
	}
	if !f.authorized(w, r) {
		return
	}
	if f.uploadDelay > 0 {
		f.mu.Unlock()
		time.Sleep(f.uploadDelay)
		f.mu.Lock()
	}
	if status := pop(&f.uploadStatuses); status != 0 {
		writeError(w, status, "injected")
		return
	}
	if f.cutUploads > 0 {
		f.cutUploads--
		_, _ = io.CopyN(io.Discard, r.Body, 64)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			f.t.Fatal(err)
		}
		_ = conn.Close()
		return
	}
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid")
		return
	}
	reader := multipart.NewReader(r.Body, params["boundary"])
	part, err := reader.NextPart()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid")
		return
	}
	var meta struct {
		Name    string `json:"name"`
		CRC32C  string `json:"crc32c"`
		MD5Hash string `json:"md5Hash"`
	}
	if err := json.NewDecoder(part).Decode(&meta); err != nil {
		writeError(w, http.StatusBadRequest, "invalid")
		return
	}
	part, err = reader.NextPart()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid")
		return
	}
	data, err := io.ReadAll(part)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid")
		return
	}
	if f.mangleTransit > 0 {
		f.mangleTransit--
		data[0] ^= 0xff
	}
	crc, md5Hash := checksumsOf(data)
	if crc != meta.CRC32C || md5Hash != meta.MD5Hash {
		writeError(w, http.StatusBadRequest, "invalid")
		return
	}
	key := bucket + "/" + meta.Name
	if _, exists := f.objects[key]; exists {
		writeError(w, http.StatusPreconditionFailed, "conditionNotMet")
		return
	}
	if f.corruptStore > 0 {
		f.corruptStore--
		data = bytes.Clone(data)
		data[len(data)-1] ^= 0x01
		crc, _ = checksumsOf(data)
	}
	f.generation++
	object := fakeObject{generation: f.generation, data: data, crc32c: crc}
	f.objects[key] = object
	f.log = append(f.log, "store "+meta.Name)
	if f.loseUploadReply > 0 {
		f.loseUploadReply--
		writeError(w, http.StatusServiceUnavailable, "backendError")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(f.metadata(bucket, meta.Name, object))
}

func (f *fakeGCS) get(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.authorized(w, r) {
		return
	}
	if status := pop(&f.statStatuses); status != 0 {
		writeError(w, status, "injected")
		return
	}
	bucket, name := r.PathValue("bucket"), r.PathValue("object")
	object, ok := f.objects[bucket+"/"+name]
	query := r.URL.Query()
	if generation := query.Get("generation"); ok && generation != "" && generation != strconv.FormatInt(object.generation, 10) {
		ok = false
	}
	if !ok {
		writeError(w, http.StatusNotFound, "notFound")
		return
	}
	if query.Get("alt") == "media" {
		if status := pop(&f.mediaStatuses); status != 0 {
			writeError(w, status, "injected")
			return
		}
		data := object.data
		if f.mangleMedia > 0 {
			f.mangleMedia--
			data = bytes.Clone(data)
			data[0] ^= 0xff
		}
		_, _ = w.Write(data)
		return
	}
	metadata := f.metadata(bucket, name, object)
	if f.staleReadBack > 0 {
		f.staleReadBack--
		metadata["crc32c"] = "AAAAAA=="
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(metadata)
}

func (f *fakeGCS) object(bucket, name string) (fakeObject, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[bucket+"/"+name]
	return object, ok
}

func (f *fakeGCS) setObject(bucket, name string, object fakeObject) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[bucket+"/"+name] = object
}

func (f *fakeGCS) deleteObject(bucket, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, bucket+"/"+name)
}

func (f *fakeGCS) storedNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}
