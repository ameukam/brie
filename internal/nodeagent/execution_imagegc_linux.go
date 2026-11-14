// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/ameukam/brie/internal/cri"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Image collection (section 10.4 of DESIGN.md). Collection runs every
// imageGCInterval when the operator sets a threshold, and an image must be
// observed unreferenced for imageGCMinUnused before it can be removed, so an
// image pulled moments earlier by another client is not collected. These are
// implementation constants, not tuned values.
const (
	imageGCInterval  = 5 * time.Minute
	imageGCMinUnused = 5 * time.Minute
)

// imageCollector removes images through CRI that no execution journal record
// and no container references. Only the collection loop uses it.
type imageCollector struct {
	client *cri.Client
	// threshold is the image filesystem usage in bytes above which
	// collection removes images.
	threshold int64
	// unusedSince maps an image ID to when collection first observed it
	// unreferenced. It is in memory only, so a restart restarts the wait.
	unusedSince map[string]time.Time
}

func (e *executor) collectImages(ctx context.Context) error {
	return e.collectImagesAt(ctx, time.Now())
}

// collectImagesAt runs one collection pass. Every input must be complete and
// every image dependency positively identified, or the pass removes nothing:
// it stops on an unreconstructed execution inventory, a running image pull, a
// failed image filesystem, image, sandbox, or container list, any existing
// sandbox, a container whose image is not identified in the image list, or a
// retained journal record without an image reference. An image is a candidate
// only when it is not pinned by the runtime, matches no reference, and was
// already unreferenced imageGCMinUnused earlier. Candidates are removed largest
// first until the reported usage, reduced by the sizes removed, is at or below
// the threshold.
func (e *executor) collectImagesAt(ctx context.Context, now time.Time) error {
	collector := e.images
	if collector == nil {
		return nil
	}
	if !e.synced.Load() || e.recovery.Load() {
		return errors.New("image collection skipped: execution inventory is not reconstructed")
	}
	// Holding the gate for the whole pass orders it against image presence
	// checks and pull starts, which hold it for reading: a check that saw an
	// image present ran after its journal record existed, and no pull starts
	// until the pass ends. A pull already running can add a reference to a
	// listed image, so the pass waits for a later run.
	e.imageGate.Lock()
	defer e.imageGate.Unlock()
	pulling, pulled := e.pulls.collectionView()
	if pulling {
		return errors.New("image collection skipped: an image pull is in progress")
	}
	filesystems, err := collector.client.ImageFsInfo(ctx)
	if err != nil {
		return fmt.Errorf("image collection: %w", err)
	}
	used := uint64(0)
	for _, usage := range filesystems {
		used += min(usage.GetUsedBytes().GetValue(), math.MaxUint64-used)
	}
	images, err := collector.client.ListImages(ctx)
	if err != nil {
		return fmt.Errorf("image collection: %w", err)
	}
	sandboxes, err := collector.client.ListPodSandbox(ctx)
	if err != nil {
		return fmt.Errorf("image collection: %w", err)
	}
	containers, err := collector.client.ListContainers(ctx)
	if err != nil {
		return fmt.Errorf("image collection: %w", err)
	}
	// CRI sandbox inventory does not name the sandbox image, and a pinned
	// image is not known to be that image. Every sandbox is a VM that may
	// depend on an image no other input names.
	if len(sandboxes) != 0 {
		return errors.New("image collection refused: CRI does not identify the images of existing sandboxes")
	}
	references := make(map[string]bool)
	for _, id := range pulled {
		references[id] = true
	}
	for _, container := range containers {
		identity := container.GetImageId()
		if identity == "" {
			identity = container.GetImageRef()
		}
		if identity == "" || !slices.ContainsFunc(images, func(image *runtimeapi.Image) bool {
			return imageReferenced(image, map[string]bool{identity: true})
		}) {
			return errors.New("image collection refused: a container's image is not identified in the image list")
		}
		for _, reference := range []string{container.GetImage().GetImage(), container.GetImage().GetUserSpecifiedImage(),
			container.GetImageRef(), container.GetImageId()} {
			if reference != "" {
				references[reference] = true
			}
		}
	}
	e.mu.Lock()
	for _, record := range e.journal.Records {
		// A finished record keeps its image until RemovePodSandbox succeeded
		// for the sandbox it names.
		if record.Stage == "finished" && (record.SandboxID == "" || record.SandboxRemoved) {
			continue
		}
		if record.Image == "" {
			e.mu.Unlock()
			return errors.New("image collection refused: a retained execution record has no image reference")
		}
		references[record.Image] = true
	}
	e.mu.Unlock()
	unused := make(map[string]*runtimeapi.Image)
	for _, image := range images {
		if !image.GetPinned() && !imageReferenced(image, references) {
			unused[image.GetId()] = image
		}
	}
	for id := range collector.unusedSince {
		if unused[id] == nil {
			delete(collector.unusedSince, id)
		}
	}
	var candidates []*runtimeapi.Image
	for id, image := range unused {
		since, seen := collector.unusedSince[id]
		if !seen {
			collector.unusedSince[id] = now
			continue
		}
		if now.Sub(since) >= imageGCMinUnused {
			candidates = append(candidates, image)
		}
	}
	threshold := uint64(collector.threshold)
	if used <= threshold {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].GetSize() != candidates[j].GetSize() {
			return candidates[i].GetSize() > candidates[j].GetSize()
		}
		return candidates[i].GetId() < candidates[j].GetId()
	})
	removed, freed := 0, uint64(0)
	defer func() {
		if removed != 0 {
			e.logger.Printf("image collection removed %d unreferenced images, %d bytes", removed, freed)
		}
	}()
	for _, image := range candidates {
		if used <= threshold {
			break
		}
		if err := collector.client.RemoveImage(ctx, image.GetId()); err != nil {
			return fmt.Errorf("image collection: %w", err)
		}
		delete(collector.unusedSince, image.GetId())
		removed, freed = removed+1, freed+image.GetSize()
		used -= min(used, image.GetSize())
	}
	return nil
}

// imageReferenced reports whether any reference names image by ID, tag, or
// digest. A digest reference also matches a repository digest with the same
// digest in another repository, so matching errs toward keeping images.
func imageReferenced(image *runtimeapi.Image, references map[string]bool) bool {
	digests := make(map[string]bool)
	for _, name := range append(append([]string{image.GetId()}, image.GetRepoTags()...), image.GetRepoDigests()...) {
		if references[name] {
			return true
		}
	}
	for _, name := range image.GetRepoDigests() {
		if digest, ok := referenceDigest(name); ok {
			digests[digest] = true
		}
	}
	for reference := range references {
		if digest, ok := referenceDigest(reference); ok && digests[digest] {
			return true
		}
	}
	return false
}

// referenceDigest returns the digest of a repository@algorithm:encoded image
// reference.
func referenceDigest(reference string) (string, bool) {
	at := strings.LastIndexByte(reference, '@')
	if at < 0 {
		return "", false
	}
	digest := reference[at+1:]
	algorithm, encoded, ok := strings.Cut(digest, ":")
	if !ok || algorithm == "" || encoded == "" {
		return "", false
	}
	return digest, true
}
