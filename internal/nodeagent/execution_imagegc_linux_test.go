// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// collectionImages is the image store of TestExecutionImageCollection. The
// running Pod's container names "sha256:image"; "mirror" shares its digest
// from another repository; "pause" is pinned like a sandbox image.
func collectionImages() []*runtimeapi.Image {
	digest := "@sha256:" + strings.Repeat("a", 64)
	return []*runtimeapi.Image{
		{Id: "sha256:image", Size: 100, RepoDigests: []string{"example.test/app" + digest}},
		{Id: "sha256:mirror", Size: 400, RepoDigests: []string{"mirror.example/app" + digest}},
		{Id: "sha256:pause", Size: 10, Pinned: true, RepoTags: []string{"registry.k8s.io/pause:3.10"}},
		{Id: "sha256:large", Size: 300, RepoDigests: []string{"example.test/large@sha256:" + strings.Repeat("1", 64)}},
		{Id: "sha256:small", Size: 200, RepoTags: []string{"example.test/small:v1"}},
	}
}

// newCollectionExecutor runs one Pod to completion of its start and enables
// image collection with the given threshold.
func newCollectionExecutor(t *testing.T, threshold int64) (*executor, *executionRuntime) {
	t.Helper()
	e, service := newExecutor(t, executionPod("app"))
	reconcilePasses(t, e, 6)
	pod, _ := e.api.CoreV1().Pods("default").Get(t.Context(), "app", metav1.GetOptions{})
	if pod.Status.Phase != corev1.PodRunning {
		t.Fatalf("Pod not running: %+v", pod.Status)
	}
	service.mu.Lock()
	service.images, service.imageFsUsed = collectionImages(), 1000
	service.mu.Unlock()
	if threshold > 0 {
		e.images = &imageCollector{client: e.runtime, threshold: threshold, unusedSince: make(map[string]time.Time)}
	}
	return e, service
}

// TestExecutionImageCollection covers section 10.4 of DESIGN.md: collection is
// disabled by default, removes only unreferenced images above the threshold,
// and treats every incomplete input as a reason to remove nothing.
func TestExecutionImageCollection(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		threshold int64
		change    func(*executor, *executionRuntime)
		// passes is the number of collections, each imageGCMinUnused apart.
		passes  int
		removed []string
		wantErr string
	}{
		{name: "disabled", passes: 2},
		{name: "below threshold", threshold: 1000, passes: 2},
		{name: "first observation", threshold: 500, passes: 1},
		{name: "largest first until at threshold", threshold: 750, passes: 2, removed: []string{"sha256:large"}},
		{name: "every candidate", threshold: 100, passes: 2, removed: []string{"sha256:large", "sha256:small"}},
		{name: "reserved record", threshold: 100, passes: 2, removed: []string{"sha256:small"},
			change: func(e *executor, _ *executionRuntime) {
				e.journal.Records["next-uid"] = &executionRecord{UID: "next-uid", Namespace: "default", Name: "next",
					Hash: strings.Repeat("0", 64), Stage: "reserved", CPU: 1, Memory: 1,
					Image: "example.test/large@sha256:" + strings.Repeat("1", 64)}
			}},
		{name: "finished record", threshold: 100, passes: 2, removed: []string{"sha256:large", "sha256:small"},
			change: func(e *executor, _ *executionRuntime) {
				e.journal.Records["done-uid"] = &executionRecord{UID: "done-uid", Namespace: "default", Name: "done",
					Hash: strings.Repeat("0", 64), Stage: "finished", CPU: 1, Memory: 1, Image: "example.test/small:v1"}
			}},
		{name: "record without image", threshold: 100, passes: 2, wantErr: "no image reference",
			change: func(e *executor, _ *executionRuntime) { e.journal.Records["app-uid"].Image = "" }},
		{name: "unpinned sandbox image", threshold: 100, passes: 2, wantErr: "pins no image",
			change: func(_ *executor, service *executionRuntime) { service.images[2].Pinned = false }},
		{name: "not reconstructed", threshold: 100, passes: 2, wantErr: "not reconstructed",
			change: func(e *executor, _ *executionRuntime) { e.synced.Store(false) }},
		{name: "recovery", threshold: 100, passes: 2, wantErr: "not reconstructed",
			change: func(e *executor, _ *executionRuntime) { e.recovery.Store(true) }},
		{name: "image filesystem failure", threshold: 100, passes: 2, wantErr: "ImageFsInfo",
			change: func(_ *executor, service *executionRuntime) { service.failImages = "ImageFsInfo" }},
		{name: "image list failure", threshold: 100, passes: 2, wantErr: "ListImages",
			change: func(_ *executor, service *executionRuntime) { service.failImages = "ListImages" }},
		{name: "sandbox list failure", threshold: 100, passes: 2, wantErr: "ListPodSandbox",
			change: func(_ *executor, service *executionRuntime) { service.failImages = "ListPodSandbox" }},
		{name: "container list failure", threshold: 100, passes: 2, wantErr: "ListContainers",
			change: func(_ *executor, service *executionRuntime) { service.failImages = "ListContainers" }},
		{name: "removal failure", threshold: 100, passes: 2, wantErr: "RemoveImage",
			change: func(_ *executor, service *executionRuntime) { service.failImages = "RemoveImage" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e, service := newCollectionExecutor(t, tt.threshold)
			now := time.Now()
			// The first observation precedes every injected change, so
			// only the last pass can remove anything.
			for pass := range tt.passes {
				if pass == tt.passes-1 && tt.change != nil {
					e.mu.Lock()
					service.mu.Lock()
					tt.change(e, service)
					service.mu.Unlock()
					e.mu.Unlock()
				}
				err := e.collectImagesAt(t.Context(), now.Add(time.Duration(pass)*imageGCMinUnused))
				if pass == tt.passes-1 && tt.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
						t.Fatalf("collection error = %v, want %q", err, tt.wantErr)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			service.mu.Lock()
			defer service.mu.Unlock()
			if !slices.Equal(service.removedImages, tt.removed) {
				t.Fatalf("removed %v, want %v", service.removedImages, tt.removed)
			}
			if tt.wantErr == "RemoveImage" && countCallsLocked(service, "RemoveImage") != 1 {
				t.Fatal("collection continued after a failed removal")
			}
		})
	}
}

func countCallsLocked(service *executionRuntime, method string) int {
	count := 0
	for _, call := range service.calls {
		if call == method {
			count++
		}
	}
	return count
}

// TestExecutionImageCollectionResetsUnusedObservation is a regression test:
// an image referenced between two passes restarts its unused wait.
func TestExecutionImageCollectionResetsUnusedObservation(t *testing.T) {
	t.Parallel()
	e, service := newCollectionExecutor(t, 100)
	now := time.Now()
	if err := e.collectImagesAt(t.Context(), now); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.journal.Records["app-uid"].Image = "example.test/small:v1"
	e.mu.Unlock()
	if err := e.collectImagesAt(t.Context(), now.Add(imageGCMinUnused)); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.journal.Records["app-uid"].Image = "example.test/app@sha256:" + strings.Repeat("a", 64)
	e.mu.Unlock()
	if err := e.collectImagesAt(t.Context(), now.Add(2*imageGCMinUnused)); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if !slices.Equal(service.removedImages, []string{"sha256:large"}) {
		t.Fatalf("removed %v; a recently referenced image was collected", service.removedImages)
	}
}

// TestExecutionImageCollectionWaitsForPresenceChecks checks that collection
// removes nothing while an image presence check holds the gate.
func TestExecutionImageCollectionWaitsForPresenceChecks(t *testing.T) {
	t.Parallel()
	e, service := newCollectionExecutor(t, 100)
	now := time.Now()
	if err := e.collectImagesAt(t.Context(), now); err != nil {
		t.Fatal(err)
	}
	e.imageGate.RLock()
	done := make(chan error, 1)
	go func() { done <- e.collectImagesAt(t.Context(), now.Add(imageGCMinUnused)) }()
	time.Sleep(100 * time.Millisecond)
	if countCalls(service, "RemoveImage") != 0 {
		e.imageGate.RUnlock()
		t.Fatal("collection removed an image during a presence check")
	}
	e.imageGate.RUnlock()
	if err := <-done; err != nil || countCalls(service, "RemoveImage") != 2 {
		t.Fatalf("collection after the presence check = %v, %d removals", err, countCalls(service, "RemoveImage"))
	}
}

func TestImageReferenced(t *testing.T) {
	t.Parallel()
	digest := "sha256:" + strings.Repeat("a", 64)
	image := &runtimeapi.Image{Id: "sha256:config", RepoTags: []string{"example.test/app:v1"},
		RepoDigests: []string{"example.test/app@" + digest}}
	for _, tt := range []struct {
		reference string
		want      bool
	}{
		{reference: "sha256:config", want: true},
		{reference: "example.test/app:v1", want: true},
		{reference: "example.test/app@" + digest, want: true},
		{reference: "mirror.example/app@" + digest, want: true},
		{reference: "example.test/app@sha256:" + strings.Repeat("b", 64)},
		{reference: "example.test/app:v2"},
		{reference: "app@"},
		{reference: "sha256:other"},
	} {
		if got := imageReferenced(image, map[string]bool{tt.reference: true}); got != tt.want {
			t.Errorf("imageReferenced(%q) = %v, want %v", tt.reference, got, tt.want)
		}
	}
}

func FuzzImageReference(f *testing.F) {
	for _, seed := range []string{"", "@", "a@b", "a@sha256:", "a@:x", "example.test/app@sha256:" + strings.Repeat("a", 64), "a@b@c:d"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, reference string) {
		digest, ok := referenceDigest(reference)
		if !ok {
			return
		}
		algorithm, encoded, found := strings.Cut(digest, ":")
		if !found || algorithm == "" || encoded == "" || strings.Contains(digest, "@") ||
			!strings.HasSuffix(reference, "@"+digest) {
			t.Fatalf("referenceDigest(%q) = %q", reference, digest)
		}
		image := &runtimeapi.Image{Id: "id", RepoDigests: []string{"other.example/repository@" + digest}}
		if !imageReferenced(image, map[string]bool{reference: true}) {
			t.Fatalf("digest reference %q did not match its digest", reference)
		}
	})
}

func FuzzImageGCThreshold(f *testing.F) {
	for _, seed := range []string{"", "50Gi", "1", "0", "-1", "0.5", "1e3", "9223372036854775807", "9223372036854775808", "invalid"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		bytes, err := imageGCThresholdBytes(value)
		if err != nil {
			if bytes != 0 {
				t.Fatal("rejected threshold returned bytes")
			}
			return
		}
		if value == "" && bytes != 0 || value != "" && bytes <= 0 {
			t.Fatalf("imageGCThresholdBytes(%q) = %d", value, bytes)
		}
	})
}
