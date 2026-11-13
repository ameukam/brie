// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ameukam/brie/internal/checkpoint"
)

const testBucket = "brie-checkpoints"

func testManifest(vmState, memory, disk checkpoint.Artifact) checkpoint.Manifest {
	return checkpoint.Manifest{
		SchemaVersion: checkpoint.ManifestSchemaVersion,
		SandboxUID:    "0f8fad5b-d9cb-469f-a165-70867728950e", SourcePodUID: "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		PlacementEpoch: 3, OperationID: "op-1", CheckpointGeneration: 1,
		Artifacts: checkpoint.Artifacts{VMState: vmState, Memory: memory,
			Disks: []checkpoint.DiskArtifact{{Device: "rootfs", Artifact: disk}}},
		Compatibility: checkpoint.Compatibility{
			HostKernel: "6.8.0", CPUModel: "Google Axion", GICVersion: "v3", FirecrackerVersion: "1.17.0",
			SnapshotFormatVersion: "8.0.0", KataVersion: "4.3.1", GuestKernel: "6.18.56", CPUTemplate: "none",
			ImageDigests: []string{"sha256:" + strings.Repeat("a", 64)}, VCPUCount: 1, MemoryMiB: 512,
		},
		DeviceLayout:          []checkpoint.Device{{Name: "rootfs", Kind: checkpoint.DeviceBlock, LogicalVolume: "container-rootfs"}},
		ResumeProtocolVersion: "brie.guestsupervisor/v1",
		SafePoint:             checkpoint.SafePoint{ID: "counter-3", ExternalEffectWatermark: 3},
		CreatedAt:             time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC),
		EncryptionKeyRef:      "google-managed", AccessPolicyRef: "lab", RetentionPolicyRef: "lab",
	}
}

func uploadAll(t *testing.T, store *Store) checkpoint.Manifest {
	t.Helper()
	var artifacts []checkpoint.Artifact
	for _, name := range []string{"vm-state", "memory", "disk-rootfs"} {
		content := bytes.Repeat([]byte(name), 1000)
		artifact, err := store.UploadArtifact(t.Context(), testBucket, "op-1/"+name, bytes.NewReader(content), int64(len(content)))
		if err != nil {
			t.Fatalf("upload %s: %v", name, err)
		}
		artifacts = append(artifacts, artifact)
	}
	return testManifest(artifacts[0], artifacts[1], artifacts[2])
}

func TestCommitAndReadManifest(t *testing.T) {
	fake := newFakeGCS(t)
	store := fake.store(t, Config{})
	manifest := uploadAll(t, store)
	if manifest.Artifacts.Memory.SizeBytes != 6000 || manifest.Artifacts.Memory.Object.Generation <= 0 {
		t.Fatalf("memory artifact = %+v", manifest.Artifacts.Memory)
	}
	ref, err := store.PublishManifest(t.Context(), testBucket, "op-1/manifest.json", manifest)
	if err != nil {
		t.Fatal(err)
	}
	if got := fake.storedNames(); len(got) != 4 || got[3] != "store op-1/manifest.json" {
		t.Fatalf("store order = %q, want the manifest last", got)
	}
	read, err := store.ReadManifest(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := checkpoint.EncodeManifest(manifest)
	got, _ := checkpoint.EncodeManifest(read)
	if !bytes.Equal(got, want) {
		t.Fatal("read manifest differs from the published one")
	}
}

func TestUploadFaults(t *testing.T) {
	content := []byte("checkpoint bytes")
	for _, tt := range []struct {
		name   string
		inject func(*fakeGCS)
		config Config
		want   error
		text   string
	}{
		{name: "transient status retried", inject: func(f *fakeGCS) { f.uploadStatuses = []int{503, 429} }},
		{name: "lost reply adopts identical object", inject: func(f *fakeGCS) { f.loseUploadReply = 1 }},
		{name: "partial upload retried", inject: func(f *fakeGCS) { f.cutUploads = 1 }},
		{name: "attempt timeout retried", inject: func(f *fakeGCS) { f.uploadDelay = 100 * time.Millisecond }, config: Config{AttemptTimeout: 20 * time.Millisecond, MaxAttempts: 2}, text: "giving up after 2 attempts"},
		{name: "partial uploads exhaust attempts", inject: func(f *fakeGCS) { f.cutUploads = 10 }, config: Config{MaxAttempts: 3}, text: "giving up after 3 attempts"},
		{name: "persistent server error", inject: func(f *fakeGCS) { f.uploadStatuses = []int{500, 500, 500, 500} }, text: "giving up after 4 attempts"},
		{name: "corrupted in transit", inject: func(f *fakeGCS) { f.mangleTransit = 1 }, text: "HTTP 400 (invalid)"},
		{name: "corrupted at rest", inject: func(f *fakeGCS) { f.corruptStore = 1 }, want: ErrIntegrity},
		{name: "read back mismatch", inject: func(f *fakeGCS) { f.staleReadBack = 1 }, want: ErrIntegrity},
		{name: "access denied not retried", inject: func(f *fakeGCS) { f.uploadStatuses = []int{403, 503} }, want: ErrAccessDenied},
		{name: "client error not retried", inject: func(f *fakeGCS) { f.uploadStatuses = []int{400, 503} }, text: "HTTP 400"},
		{name: "read back missing", inject: func(f *fakeGCS) { f.statStatuses = []int{404} }, want: ErrObjectNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeGCS(t)
			tt.inject(fake)
			store := fake.store(t, tt.config)
			artifact, err := store.UploadArtifact(t.Context(), testBucket, "op-1/memory", bytes.NewReader(content), int64(len(content)))
			if err != nil && strings.Contains(err.Error(), fakeToken) {
				t.Fatalf("error leaks the token: %v", err)
			}
			switch {
			case tt.want != nil:
				if !errors.Is(err, tt.want) {
					t.Fatalf("UploadArtifact error = %v, want %v", err, tt.want)
				}
			case tt.text != "":
				if err == nil || !strings.Contains(err.Error(), tt.text) {
					t.Fatalf("UploadArtifact error = %v, want %q", err, tt.text)
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
				stored, ok := fake.object(testBucket, "op-1/memory")
				if !ok || !bytes.Equal(stored.data, content) || stored.generation != artifact.Object.Generation {
					t.Fatalf("stored object = %+v, artifact %+v", stored, artifact)
				}
				if names := fake.storedNames(); len(names) != 1 {
					t.Fatalf("stored %d times: %q", len(names), names)
				}
			}
		})
	}
}

func TestUploadNeverOverwrites(t *testing.T) {
	fake := newFakeGCS(t)
	store := fake.store(t, Config{})
	original := []byte("generation one")
	first, err := store.UploadArtifact(t.Context(), testBucket, "op-1/disk", bytes.NewReader(original), int64(len(original)))
	if err != nil {
		t.Fatal(err)
	}
	replacement := []byte("generation two")
	if _, err := store.UploadArtifact(t.Context(), testBucket, "op-1/disk", bytes.NewReader(replacement), int64(len(replacement))); !errors.Is(err, ErrObjectExists) {
		t.Fatalf("overwrite error = %v, want ErrObjectExists", err)
	}
	stored, _ := fake.object(testBucket, "op-1/disk")
	if !bytes.Equal(stored.data, original) || stored.generation != first.Object.Generation {
		t.Fatal("existing generation was replaced")
	}
	// Re-uploading identical bytes returns the existing generation.
	again, err := store.UploadArtifact(t.Context(), testBucket, "op-1/disk", bytes.NewReader(original), int64(len(original)))
	if err != nil || again != first {
		t.Fatalf("identical re-upload = %+v, %v", again, err)
	}
}

func TestUploadRejectsInput(t *testing.T) {
	store := newFakeGCS(t).store(t, Config{})
	content := bytes.NewReader([]byte("abc"))
	for _, tt := range []struct {
		name, bucket, object string
		size                 int64
		want                 string
	}{
		{name: "bucket", bucket: "B", object: "a", size: 3, want: "bucket"},
		{name: "object", bucket: testBucket, object: "", size: 3, want: "object name"},
		{name: "size", bucket: testBucket, object: "a", size: 0, want: "size"},
		{name: "short content", bucket: testBucket, object: "a", size: 4, want: "3 bytes, want 4"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := store.UploadArtifact(t.Context(), tt.bucket, tt.object, content, tt.size); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("UploadArtifact error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestPublishManifestRequiresVerifiedArtifacts(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*fakeGCS, *checkpoint.Manifest)
		want   error
		text   string
	}{
		{name: "missing artifact", mutate: func(f *fakeGCS, m *checkpoint.Manifest) {
			f.deleteObject(testBucket, m.Artifacts.Memory.Object.Name)
		}, want: ErrObjectNotFound},
		{name: "replaced generation", mutate: func(f *fakeGCS, m *checkpoint.Manifest) {
			object, _ := f.object(testBucket, m.Artifacts.VMState.Object.Name)
			object.generation++
			f.setObject(testBucket, m.Artifacts.VMState.Object.Name, object)
		}, want: ErrObjectNotFound},
		{name: "size mismatch", mutate: func(_ *fakeGCS, m *checkpoint.Manifest) {
			m.Artifacts.Disks[0].Artifact.SizeBytes++
		}, want: ErrIntegrity},
		{name: "checksum mismatch", mutate: func(_ *fakeGCS, m *checkpoint.Manifest) {
			m.Artifacts.Memory.CRC32C = "AAAAAA=="
		}, want: ErrIntegrity},
		{name: "invalid manifest", mutate: func(_ *fakeGCS, m *checkpoint.Manifest) { m.OperationID = "" }, text: "invalid manifest"},
		{name: "name collision", mutate: func(_ *fakeGCS, m *checkpoint.Manifest) {
			m.Artifacts.Memory.Object.Name = "op-1/manifest.json"
		}, text: "collides"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeGCS(t)
			store := fake.store(t, Config{})
			manifest := uploadAll(t, store)
			tt.mutate(fake, &manifest)
			_, err := store.PublishManifest(t.Context(), testBucket, "op-1/manifest.json", manifest)
			if tt.want != nil && !errors.Is(err, tt.want) || tt.text != "" && (err == nil || !strings.Contains(err.Error(), tt.text)) {
				t.Fatalf("PublishManifest error = %v, want %v %q", err, tt.want, tt.text)
			}
			if _, published := fake.object(testBucket, "op-1/manifest.json"); published {
				t.Fatal("manifest published despite an unverified artifact")
			}
		})
	}
	store := newFakeGCS(t).store(t, Config{})
	manifest := testManifest(checkpoint.Artifact{}, checkpoint.Artifact{}, checkpoint.Artifact{})
	if _, err := store.PublishManifest(t.Context(), "B", "m", manifest); err == nil {
		t.Fatal("PublishManifest accepted an invalid manifest")
	}
}

func TestPublishManifestValidatesTarget(t *testing.T) {
	fake := newFakeGCS(t)
	store := fake.store(t, Config{})
	manifest := uploadAll(t, store)
	if _, err := store.PublishManifest(t.Context(), "B", "m", manifest); err == nil || !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("bad bucket error = %v", err)
	}
	if _, err := store.PublishManifest(t.Context(), testBucket, "", manifest); err == nil || !strings.Contains(err.Error(), "object name") {
		t.Fatalf("bad name error = %v", err)
	}
}

func TestReadManifestRejectsIncompleteCheckpoints(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*fakeGCS, checkpoint.Manifest, checkpoint.ObjectRef) checkpoint.ObjectRef
		want   error
		text   string
	}{
		{name: "missing manifest", mutate: func(f *fakeGCS, _ checkpoint.Manifest, ref checkpoint.ObjectRef) checkpoint.ObjectRef {
			f.deleteObject(testBucket, ref.Name)
			return ref
		}, want: ErrObjectNotFound},
		{name: "wrong generation", mutate: func(_ *fakeGCS, _ checkpoint.Manifest, ref checkpoint.ObjectRef) checkpoint.ObjectRef {
			ref.Generation++
			return ref
		}, want: ErrObjectNotFound},
		{name: "missing artifact", mutate: func(f *fakeGCS, m checkpoint.Manifest, ref checkpoint.ObjectRef) checkpoint.ObjectRef {
			f.deleteObject(testBucket, m.Artifacts.Disks[0].Artifact.Object.Name)
			return ref
		}, text: "manifest incomplete"},
		{name: "corrupt manifest bytes", mutate: func(f *fakeGCS, _ checkpoint.Manifest, ref checkpoint.ObjectRef) checkpoint.ObjectRef {
			object, _ := f.object(testBucket, ref.Name)
			object.data = bytes.Replace(object.data, []byte(`"placement_epoch":3`), []byte(`"placement_epoch":4`), 1)
			f.setObject(testBucket, ref.Name, object)
			return ref
		}, want: ErrIntegrity},
		{name: "non-canonical manifest with matching checksums", mutate: func(f *fakeGCS, _ checkpoint.Manifest, ref checkpoint.ObjectRef) checkpoint.ObjectRef {
			object, _ := f.object(testBucket, ref.Name)
			object.data = append(object.data, '\n')
			object.crc32c, _ = checksumsOf(object.data)
			f.setObject(testBucket, ref.Name, object)
			return ref
		}, text: "not canonically encoded"},
		{name: "oversized manifest", mutate: func(f *fakeGCS, _ checkpoint.Manifest, ref checkpoint.ObjectRef) checkpoint.ObjectRef {
			object, _ := f.object(testBucket, ref.Name)
			object.data = bytes.Repeat([]byte(" "), checkpoint.MaxManifestBytes+1)
			object.crc32c, _ = checksumsOf(object.data)
			f.setObject(testBucket, ref.Name, object)
			return ref
		}, text: "outside 1 to"},
		{name: "invalid ref", mutate: func(_ *fakeGCS, _ checkpoint.Manifest, ref checkpoint.ObjectRef) checkpoint.ObjectRef {
			ref.Generation = 0
			return ref
		}, text: "generation must be positive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeGCS(t)
			store := fake.store(t, Config{})
			manifest := uploadAll(t, store)
			ref, err := store.PublishManifest(t.Context(), testBucket, "op-1/manifest.json", manifest)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.ReadManifest(t.Context(), tt.mutate(fake, manifest, ref))
			if tt.want != nil && !errors.Is(err, tt.want) || tt.text != "" && (err == nil || !strings.Contains(err.Error(), tt.text)) {
				t.Fatalf("ReadManifest error = %v, want %v %q", err, tt.want, tt.text)
			}
		})
	}
}

func TestVerifyArtifactRejectsInvalidRef(t *testing.T) {
	store := newFakeGCS(t).store(t, Config{})
	if err := store.VerifyArtifact(t.Context(), checkpoint.Artifact{}); err == nil {
		t.Fatal("VerifyArtifact accepted an empty ref")
	}
}

func TestRetryHonorsContext(t *testing.T) {
	fake := newFakeGCS(t)
	fake.uploadStatuses = []int{503, 503, 503, 503}
	store := fake.store(t, Config{InitialBackoff: time.Hour, MaxBackoff: time.Hour})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := store.UploadArtifact(ctx, testBucket, "a", bytes.NewReader([]byte("a")), 1)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "after 1 attempts") {
		t.Fatalf("UploadArtifact error = %v", err)
	}
}

func TestTokenFailureIsRetriedAndRedacted(t *testing.T) {
	fake := newFakeGCS(t)
	calls := 0
	tokens := tokenFunc(func() (AccessToken, error) {
		calls++
		if calls == 1 {
			return AccessToken{}, errors.New("metadata server unavailable")
		}
		return NewAccessToken(fakeToken, time.Now().Add(time.Hour))
	})
	store := fake.store(t, Config{Tokens: tokens})
	if _, err := store.UploadArtifact(t.Context(), testBucket, "a", bytes.NewReader([]byte("a")), 1); err != nil {
		t.Fatal(err)
	}

	wrong, _ := NewAccessToken("ya29.wrong-SECRET", time.Now().Add(time.Hour))
	denied := fake.store(t, Config{Tokens: tokenFunc(func() (AccessToken, error) { return wrong, nil })})
	_, err := denied.UploadArtifact(t.Context(), testBucket, "b", bytes.NewReader([]byte("b")), 1)
	if !errors.Is(err, ErrAccessDenied) || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "echo") {
		t.Fatalf("denied error = %v", err)
	}
}

func TestStoreRefusesRedirects(t *testing.T) {
	followed := false
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed = true }))
	defer elsewhere.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	store, err := New(Config{Tokens: staticTokens(t), Endpoint: redirector.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UploadArtifact(t.Context(), testBucket, "a", bytes.NewReader([]byte("a")), 1); err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("redirect error = %v", err)
	}
	if followed {
		t.Fatal("bearer token request followed a redirect")
	}
}

func TestStoreRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), maxMetadataBytes+1))
	}))
	defer server.Close()
	store, err := New(Config{Tokens: staticTokens(t), Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ref := checkpoint.Artifact{Object: checkpoint.ObjectRef{Bucket: testBucket, Name: "a", Generation: 1}}
	if err := store.VerifyArtifact(t.Context(), ref); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized response error = %v", err)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	tokens := staticTokens(t)
	for _, tt := range []struct {
		name   string
		config Config
		want   string
	}{
		{name: "no tokens", config: Config{}, want: "token source"},
		{name: "relative endpoint", config: Config{Tokens: tokens, Endpoint: "storage"}, want: "endpoint"},
		{name: "endpoint credentials", config: Config{Tokens: tokens, Endpoint: "https://u:p@example.com"}, want: "endpoint"},
		{name: "endpoint query", config: Config{Tokens: tokens, Endpoint: "https://example.com/?a=b"}, want: "endpoint"},
		{name: "ftp endpoint", config: Config{Tokens: tokens, Endpoint: "ftp://example.com"}, want: "endpoint"},
		{name: "too many attempts", config: Config{Tokens: tokens, MaxAttempts: MaxAttemptsLimit + 1}, want: "attempts"},
		{name: "negative attempts", config: Config{Tokens: tokens, MaxAttempts: -1}, want: "attempts"},
		{name: "negative timeout", config: Config{Tokens: tokens, AttemptTimeout: -time.Second}, want: "timeouts"},
		{name: "backoff order", config: Config{Tokens: tokens, InitialBackoff: time.Second, MaxBackoff: time.Millisecond}, want: "backoff"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.config); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New error = %v, want %q", err, tt.want)
			}
		})
	}
	store, err := New(Config{Tokens: tokens})
	if err != nil || store.endpoint != DefaultEndpoint || store.maxAttempts != DefaultMaxAttempts {
		t.Fatalf("defaults = %+v, %v", store, err)
	}
}

func TestDecodeObjectMetadata(t *testing.T) {
	valid := `{"bucket":"brie-checkpoints","name":"a/b","generation":"12","size":"3","crc32c":"AAAAAA==","md5Hash":"AAAAAAAAAAAAAAAAAAAAAA==","kind":"storage#object"}`
	if object, err := decodeObjectMetadata([]byte(valid)); err != nil || object.Generation != 12 || object.Size != 3 {
		t.Fatalf("decodeObjectMetadata = %+v, %v", object, err)
	}
	for _, tt := range []struct {
		name, data, want string
	}{
		{name: "not JSON", data: "x", want: "not valid JSON"},
		{name: "numeric generation", data: strings.Replace(valid, `"12"`, `12`, 1), want: "not valid JSON"},
		{name: "bad bucket", data: strings.Replace(valid, `brie-checkpoints`, `B`, 1), want: "bucket or name"},
		{name: "zero generation", data: strings.Replace(valid, `"12"`, `"0"`, 1), want: "generation or size"},
		{name: "negative size", data: strings.Replace(valid, `"3"`, `"-3"`, 1), want: "generation or size"},
		{name: "bad crc", data: strings.Replace(valid, `AAAAAA==`, `AAA`, 1), want: "crc32c"},
		{name: "missing md5", data: strings.Replace(valid, `AAAAAAAAAAAAAAAAAAAAAA==`, ``, 1), want: "md5Hash"},
		{name: "oversized", data: strings.Repeat(" ", maxMetadataBytes+1), want: "too large"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeObjectMetadata([]byte(tt.data)); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("decodeObjectMetadata error = %v, want %q", err, tt.want)
			}
		})
	}
}

func FuzzDecodeObjectMetadata(f *testing.F) {
	f.Add([]byte(`{"bucket":"brie-checkpoints","name":"a/b","generation":"12","size":"3","crc32c":"AAAAAA==","md5Hash":"AAAAAAAAAAAAAAAAAAAAAA=="}`))
	f.Add([]byte(`{"generation":"-1"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		object, err := decodeObjectMetadata(data)
		if err != nil {
			return
		}
		if object.Generation <= 0 || object.Size < 0 || !checkpoint.ValidCRC32C(object.CRC32C) ||
			checkpoint.ValidateBucket(object.Bucket) != nil || checkpoint.ValidateObjectName(object.Name) != nil {
			t.Fatalf("accepted invalid metadata %+v", object)
		}
	})
}

func TestStatusErrorOmitsResponseText(t *testing.T) {
	body := []byte(`{"error":{"message":"` + fakeToken + `","errors":[{"reason":"bad reason with spaces"}]}}`)
	err := statusError("read", 400, body)
	if text := fmt.Sprint(err); strings.Contains(text, fakeToken) || strings.Contains(text, "spaces") || text != "read: HTTP 400" {
		t.Fatalf("statusError = %q", text)
	}
}
