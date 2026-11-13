// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package gcs stores checkpoint artifacts and manifests in Google Cloud
// Storage through its JSON API, using only the standard library.
//
// Uploads are immutable (ifGenerationMatch=0) and verified by size, CRC32C,
// MD5, and object generation read back from the store. A manifest is
// published last, after every artifact it references verifies. The package
// is tested only against an in-process fake; it has not been run against a
// real bucket.
package gcs

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ameukam/brie/internal/checkpoint"
)

// DefaultEndpoint is the Cloud Storage JSON API endpoint.
const DefaultEndpoint = "https://storage.googleapis.com"

// Defaults and limits for retries and responses.
const (
	DefaultMaxAttempts    = 4
	MaxAttemptsLimit      = 10
	DefaultAttemptTimeout = 10 * time.Minute
	DefaultInitialBackoff = 200 * time.Millisecond
	DefaultMaxBackoff     = 5 * time.Second
	maxMetadataBytes      = 64 << 10
)

// Errors callers can match with errors.Is.
var (
	// ErrObjectExists reports an existing object whose content differs.
	ErrObjectExists = errors.New("object already exists with different content")
	// ErrObjectNotFound reports a missing object or generation.
	ErrObjectNotFound = errors.New("object generation not found")
	// ErrIntegrity reports a size, checksum, or generation mismatch.
	ErrIntegrity = errors.New("stored object does not match the expected size, checksums, or generation")
	// ErrAccessDenied reports an authentication or authorization failure.
	ErrAccessDenied = errors.New("storage access denied")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Config configures a Store. Zero durations and counts select defaults.
type Config struct {
	HTTPClient     *http.Client
	Tokens         TokenSource
	Endpoint       string
	MaxAttempts    int
	AttemptTimeout time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// Store reads and writes checkpoint objects. Each request attempt has its own
// deadline, retries are bounded, and redirects are refused.
type Store struct {
	client         *http.Client
	tokens         TokenSource
	endpoint       string
	maxAttempts    int
	attemptTimeout time.Duration
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

// New validates config and creates a Store.
func New(config Config) (*Store, error) {
	if config.Tokens == nil {
		return nil, errors.New("storage token source is required")
	}
	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return nil, errors.New("storage endpoint must be an absolute http(s) URL without credentials, query, or fragment")
	}
	store := &Store{
		client: noRedirects(config.HTTPClient), tokens: config.Tokens,
		endpoint:    strings.TrimSuffix(endpoint, "/"),
		maxAttempts: config.MaxAttempts, attemptTimeout: config.AttemptTimeout,
		initialBackoff: config.InitialBackoff, maxBackoff: config.MaxBackoff,
	}
	if store.maxAttempts == 0 {
		store.maxAttempts = DefaultMaxAttempts
	}
	if store.attemptTimeout == 0 {
		store.attemptTimeout = DefaultAttemptTimeout
	}
	if store.initialBackoff == 0 {
		store.initialBackoff = DefaultInitialBackoff
	}
	if store.maxBackoff == 0 {
		store.maxBackoff = DefaultMaxBackoff
	}
	if store.maxAttempts < 1 || store.maxAttempts > MaxAttemptsLimit {
		return nil, fmt.Errorf("storage attempts must be between 1 and %d", MaxAttemptsLimit)
	}
	if store.attemptTimeout < 0 || store.initialBackoff < 0 || store.maxBackoff < store.initialBackoff {
		return nil, errors.New("storage timeouts must be positive and the maximum backoff at least the initial backoff")
	}
	return store, nil
}

// checksums describe local bytes before upload.
type checksums struct {
	size   int64
	sha256 string
	crc32c string
	md5    string
}

func computeChecksums(content io.ReaderAt, size int64) (checksums, error) {
	sha := sha256.New()
	crc := crc32.New(castagnoli)
	// MD5 matches the store's own transfer checksum; SHA-256 is the
	// manifest digest.
	sum := md5.New()
	n, err := io.Copy(io.MultiWriter(sha, crc, sum), io.NewSectionReader(content, 0, size))
	if err != nil {
		return checksums{}, fmt.Errorf("read artifact content: %w", err)
	}
	if n != size {
		return checksums{}, fmt.Errorf("artifact content has %d bytes, want %d", n, size)
	}
	return checksums{
		size: size, sha256: hex.EncodeToString(sha.Sum(nil)),
		crc32c: encodeCRC32C(crc.Sum32()), md5: base64.StdEncoding.EncodeToString(sum.Sum(nil)),
	}, nil
}

func encodeCRC32C(value uint32) string {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], value)
	return base64.StdEncoding.EncodeToString(raw[:])
}

func (c checksums) matches(object objectMetadata) bool {
	return object.Size == c.size && object.CRC32C == c.crc32c && object.MD5Hash == c.md5
}

// objectMetadata is the subset of the JSON API object resource Brie checks.
type objectMetadata struct {
	Bucket     string `json:"bucket"`
	Name       string `json:"name"`
	Generation int64  `json:"generation,string"`
	Size       int64  `json:"size,string"`
	CRC32C     string `json:"crc32c"`
	MD5Hash    string `json:"md5Hash"`
}

// decodeObjectMetadata parses an object resource. Unknown fields are allowed
// because the resource is defined by the storage service.
func decodeObjectMetadata(body []byte) (objectMetadata, error) {
	if len(body) > maxMetadataBytes {
		return objectMetadata{}, errors.New("object metadata is too large")
	}
	var object objectMetadata
	if err := json.Unmarshal(body, &object); err != nil {
		return objectMetadata{}, errors.New("object metadata is not valid JSON")
	}
	if checkpoint.ValidateBucket(object.Bucket) != nil || checkpoint.ValidateObjectName(object.Name) != nil {
		return objectMetadata{}, errors.New("object metadata has an invalid bucket or name")
	}
	if object.Generation <= 0 || object.Size < 0 {
		return objectMetadata{}, errors.New("object metadata has an invalid generation or size")
	}
	if !checkpoint.ValidCRC32C(object.CRC32C) {
		return objectMetadata{}, errors.New("object metadata has an invalid crc32c")
	}
	if md5Raw, err := base64.StdEncoding.Strict().DecodeString(object.MD5Hash); err != nil || len(md5Raw) != md5.Size {
		return objectMetadata{}, errors.New("object metadata has an invalid md5Hash")
	}
	return object, nil
}

// UploadArtifact uploads size bytes of content as a new object and verifies
// the stored generation. It never overwrites: if the object exists with the
// same size and checksums (for example after a lost reply) that generation is
// returned, and otherwise ErrObjectExists.
func (s *Store) UploadArtifact(ctx context.Context, bucket, name string, content io.ReaderAt, size int64) (checkpoint.Artifact, error) {
	if err := checkpoint.ValidateBucket(bucket); err != nil {
		return checkpoint.Artifact{}, err
	}
	if err := checkpoint.ValidateObjectName(name); err != nil {
		return checkpoint.Artifact{}, err
	}
	if size <= 0 {
		return checkpoint.Artifact{}, errors.New("artifact size must be positive")
	}
	sums, err := computeChecksums(content, size)
	if err != nil {
		return checkpoint.Artifact{}, err
	}
	generation, err := s.upload(ctx, bucket, name, content, sums)
	if err != nil {
		return checkpoint.Artifact{}, err
	}
	return checkpoint.Artifact{
		Object:    checkpoint.ObjectRef{Bucket: bucket, Name: name, Generation: generation},
		SizeBytes: size, SHA256: sums.sha256, CRC32C: sums.crc32c,
	}, nil
}

func (s *Store) upload(ctx context.Context, bucket, name string, content io.ReaderAt, sums checksums) (int64, error) {
	var generation int64
	err := s.retry(ctx, func(ctx context.Context) error {
		status, body, err := s.send(ctx, func(ctx context.Context) (*http.Request, error) {
			return s.uploadRequest(ctx, bucket, name, content, sums)
		}, maxMetadataBytes)
		if err != nil {
			return err
		}
		switch status {
		case http.StatusOK:
			object, err := decodeObjectMetadata(body)
			if err != nil {
				return err
			}
			if object.Bucket != bucket || object.Name != name || !sums.matches(object) {
				return fmt.Errorf("upload %s: %w", name, ErrIntegrity)
			}
			generation = object.Generation
			return nil
		case http.StatusPreconditionFailed:
			existing, err := s.statOnce(ctx, bucket, name, 0)
			if err != nil {
				return err
			}
			if !sums.matches(existing) {
				return fmt.Errorf("upload %s: %w", name, ErrObjectExists)
			}
			generation = existing.Generation
			return nil
		default:
			return statusError("upload "+name, status, body)
		}
	})
	if err != nil {
		return 0, err
	}
	stored, err := s.stat(ctx, bucket, name, generation)
	if err != nil {
		return 0, fmt.Errorf("verify upload %s: %w", name, err)
	}
	if stored.Generation != generation || !sums.matches(stored) {
		return 0, fmt.Errorf("verify upload %s: %w", name, ErrIntegrity)
	}
	return generation, nil
}

// uploadRequest builds a multipart upload whose metadata carries the CRC32C
// and MD5 checksums, so the service rejects bytes that differ from them. The
// random 128-bit boundary cannot be predicted by the guest whose memory is
// uploaded. Each attempt resends the whole object; resumable uploads for
// large memory files are not implemented.
func (s *Store) uploadRequest(ctx context.Context, bucket, name string, content io.ReaderAt, sums checksums) (*http.Request, error) {
	metadata, err := json.Marshal(map[string]string{"name": name, "crc32c": sums.crc32c, "md5Hash": sums.md5})
	if err != nil {
		return nil, fmt.Errorf("encode upload metadata: %w", err)
	}
	var boundaryBytes [16]byte
	if _, err := rand.Read(boundaryBytes[:]); err != nil {
		return nil, fmt.Errorf("generate multipart boundary: %w", err)
	}
	boundary := hex.EncodeToString(boundaryBytes[:])
	head := "--" + boundary + "\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n" + string(metadata) +
		"\r\n--" + boundary + "\r\nContent-Type: application/octet-stream\r\n\r\n"
	tail := "\r\n--" + boundary + "--\r\n"
	query := url.Values{"uploadType": {"multipart"}, "ifGenerationMatch": {"0"}}
	target := s.endpoint + "/upload/storage/v1/b/" + url.PathEscape(bucket) + "/o?" + query.Encode()
	body := io.MultiReader(strings.NewReader(head), io.NewSectionReader(content, 0, sums.size), strings.NewReader(tail))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, body)
	if err != nil {
		return nil, fmt.Errorf("build upload request: %w", err)
	}
	request.ContentLength = int64(len(head)) + sums.size + int64(len(tail))
	request.Header.Set("Content-Type", "multipart/related; boundary="+boundary)
	return request, nil
}

// stat reads an object generation's metadata with retries. Generation zero
// reads the latest generation.
func (s *Store) stat(ctx context.Context, bucket, name string, generation int64) (objectMetadata, error) {
	var object objectMetadata
	err := s.retry(ctx, func(ctx context.Context) error {
		var err error
		object, err = s.statOnce(ctx, bucket, name, generation)
		return err
	})
	return object, err
}

func (s *Store) statOnce(ctx context.Context, bucket, name string, generation int64) (objectMetadata, error) {
	status, body, err := s.send(ctx, func(ctx context.Context) (*http.Request, error) {
		return s.objectRequest(ctx, bucket, name, generation, false)
	}, maxMetadataBytes)
	if err != nil {
		return objectMetadata{}, err
	}
	if status != http.StatusOK {
		return objectMetadata{}, statusError("read metadata of "+name, status, body)
	}
	object, err := decodeObjectMetadata(body)
	if err != nil {
		return objectMetadata{}, err
	}
	if object.Bucket != bucket || object.Name != name || (generation != 0 && object.Generation != generation) {
		return objectMetadata{}, fmt.Errorf("read metadata of %s: %w", name, ErrIntegrity)
	}
	return object, nil
}

func (s *Store) objectRequest(ctx context.Context, bucket, name string, generation int64, media bool) (*http.Request, error) {
	query := url.Values{}
	if generation != 0 {
		query.Set("generation", strconv.FormatInt(generation, 10))
	}
	if media {
		query.Set("alt", "media")
	}
	target := s.endpoint + "/storage/v1/b/" + url.PathEscape(bucket) + "/o/" + url.PathEscape(name)
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("build object request: %w", err)
	}
	return request, nil
}

// VerifyArtifact checks that the artifact's object generation exists with
// the recorded size and CRC32C.
func (s *Store) VerifyArtifact(ctx context.Context, artifact checkpoint.Artifact) error {
	if err := artifact.Object.Validate(); err != nil {
		return err
	}
	object, err := s.stat(ctx, artifact.Object.Bucket, artifact.Object.Name, artifact.Object.Generation)
	if err != nil {
		return fmt.Errorf("verify artifact %s: %w", artifact.Object.Name, err)
	}
	if object.Size != artifact.SizeBytes || object.CRC32C != artifact.CRC32C {
		return fmt.Errorf("verify artifact %s: %w", artifact.Object.Name, ErrIntegrity)
	}
	return nil
}

// PublishManifest verifies every artifact the manifest references and only
// then uploads the canonical manifest as a new immutable object.
func (s *Store) PublishManifest(ctx context.Context, bucket, name string, manifest checkpoint.Manifest) (checkpoint.ObjectRef, error) {
	encoded, err := checkpoint.EncodeManifest(manifest)
	if err != nil {
		return checkpoint.ObjectRef{}, err
	}
	if err := checkpoint.ValidateBucket(bucket); err != nil {
		return checkpoint.ObjectRef{}, err
	}
	if err := checkpoint.ValidateObjectName(name); err != nil {
		return checkpoint.ObjectRef{}, err
	}
	for _, artifact := range manifest.ArtifactList() {
		if artifact.Object.Bucket == bucket && artifact.Object.Name == name {
			return checkpoint.ObjectRef{}, errors.New("manifest object name collides with an artifact")
		}
		if err := s.VerifyArtifact(ctx, artifact); err != nil {
			return checkpoint.ObjectRef{}, fmt.Errorf("manifest not published: %w", err)
		}
	}
	sums, err := computeChecksums(bytes.NewReader(encoded), int64(len(encoded)))
	if err != nil {
		return checkpoint.ObjectRef{}, err
	}
	generation, err := s.upload(ctx, bucket, name, bytes.NewReader(encoded), sums)
	if err != nil {
		return checkpoint.ObjectRef{}, err
	}
	return checkpoint.ObjectRef{Bucket: bucket, Name: name, Generation: generation}, nil
}

// ReadManifest downloads one manifest generation, checks its stored size and
// checksums, decodes it strictly, and verifies every referenced artifact.
// Only a complete committed generation is returned.
func (s *Store) ReadManifest(ctx context.Context, ref checkpoint.ObjectRef) (checkpoint.Manifest, error) {
	if err := ref.Validate(); err != nil {
		return checkpoint.Manifest{}, err
	}
	object, err := s.stat(ctx, ref.Bucket, ref.Name, ref.Generation)
	if err != nil {
		return checkpoint.Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	if object.Size <= 0 || object.Size > checkpoint.MaxManifestBytes {
		return checkpoint.Manifest{}, fmt.Errorf("read manifest: size %d is outside 1 to %d bytes", object.Size, checkpoint.MaxManifestBytes)
	}
	var data []byte
	err = s.retry(ctx, func(ctx context.Context) error {
		status, body, err := s.send(ctx, func(ctx context.Context) (*http.Request, error) {
			return s.objectRequest(ctx, ref.Bucket, ref.Name, ref.Generation, true)
		}, checkpoint.MaxManifestBytes)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return statusError("download manifest", status, body)
		}
		data = body
		return nil
	})
	if err != nil {
		return checkpoint.Manifest{}, err
	}
	sums, err := computeChecksums(bytes.NewReader(data), int64(len(data)))
	if err != nil || !sums.matches(object) {
		return checkpoint.Manifest{}, fmt.Errorf("download manifest: %w", ErrIntegrity)
	}
	manifest, err := checkpoint.DecodeManifest(data)
	if err != nil {
		return checkpoint.Manifest{}, err
	}
	for _, artifact := range manifest.ArtifactList() {
		if err := s.VerifyArtifact(ctx, artifact); err != nil {
			return checkpoint.Manifest{}, fmt.Errorf("manifest incomplete: %w", err)
		}
	}
	return manifest, nil
}

// retryableError marks a transient failure.
type retryableError struct{ err error }

func (e retryableError) Error() string { return e.err.Error() }
func (e retryableError) Unwrap() error { return e.err }

// retry runs attempt at most maxAttempts times, each under attemptTimeout,
// backing off exponentially between transient failures.
func (s *Store) retry(ctx context.Context, attempt func(context.Context) error) error {
	backoff := s.initialBackoff
	var err error
	for i := 0; i < s.maxAttempts; i++ {
		if i > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("%w (after %d attempts: %w)", ctx.Err(), i, err)
			}
			backoff = min(2*backoff, s.maxBackoff)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, s.attemptTimeout)
		err = attempt(attemptCtx)
		cancel()
		if _, transient := errors.AsType[retryableError](err); !transient {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
	}
	return fmt.Errorf("giving up after %d attempts: %w", s.maxAttempts, err)
}

// send authorizes and runs one request and reads at most limit body bytes.
// Transport failures are transient. The token is set only as a header and
// never appears in returned errors.
func (s *Store) send(ctx context.Context, build func(context.Context) (*http.Request, error), limit int64) (int, []byte, error) {
	token, err := s.tokens.Token(ctx)
	if err != nil {
		return 0, nil, retryableError{fmt.Errorf("obtain storage access token: %w", err)}
	}
	request, err := build(ctx)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token.value)
	request.Header.Set("User-Agent", "brie-checkpoint")
	response, err := s.client.Do(request)
	if err != nil {
		return 0, nil, retryableError{fmt.Errorf("storage request: %w", err)}
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return 0, nil, retryableError{fmt.Errorf("read storage response: %w", err)}
	}
	if int64(len(body)) > limit {
		return 0, nil, fmt.Errorf("storage response exceeds %d bytes", limit)
	}
	return response.StatusCode, body, nil
}

// knownReasons lists the Cloud Storage error reasons an error may name. A
// reason is reported only through this fixed table, so response text such as
// a reflected credential or object data never reaches an error.
var knownReasons = map[string]string{
	"authError":               "authError",
	"backendError":            "backendError",
	"badRequest":              "badRequest",
	"conditionNotMet":         "conditionNotMet",
	"forbidden":               "forbidden",
	"insufficientPermissions": "insufficientPermissions",
	"invalid":                 "invalid",
	"notFound":                "notFound",
	"rateLimitExceeded":       "rateLimitExceeded",
	"required":                "required",
	"unauthorized":            "unauthorized",
}

// statusError classifies a non-success status. It reports the status and a
// known machine-readable reason only, never the response text.
func statusError(action string, status int, body []byte) error {
	reason := ""
	var parsed struct {
		Error struct {
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &parsed) == nil && len(parsed.Error.Errors) > 0 {
		if known, ok := knownReasons[parsed.Error.Errors[0].Reason]; ok {
			reason = " (" + known + ")"
		}
	}
	err := fmt.Errorf("%s: HTTP %d%s", action, status, reason)
	switch {
	case status == http.StatusNotFound:
		return fmt.Errorf("%w: %w", err, ErrObjectNotFound)
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return fmt.Errorf("%w: %w", err, ErrAccessDenied)
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500:
		return retryableError{err}
	default:
		return err
	}
}
