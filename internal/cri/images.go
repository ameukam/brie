// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"errors"
	"fmt"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// ListImages reads every image the runtime stores, without a filter. An error
// means the image inventory is unavailable, not empty, and never returns
// partial results. A complete list says which images exist; whether one is
// unused needs the caller's own complete reference inventory.
func (c *Client) ListImages(ctx context.Context) ([]*runtimeapi.Image, error) {
	response, err := invoke(ctx, c, "ListImages", func(ctx context.Context) (*runtimeapi.ListImagesResponse, error) {
		return c.images.ListImages(ctx, &runtimeapi.ListImagesRequest{})
	})
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, image := range response.GetImages() {
		if err := validateImage(image); err != nil {
			return nil, fmt.Errorf("CRI ListImages: %w", err)
		}
		if seen[image.GetId()] {
			return nil, errors.New("CRI ListImages: duplicate image ID")
		}
		seen[image.GetId()] = true
	}
	return response.GetImages(), nil
}

// RemoveImage removes the image with the exact runtime ID or reference. CRI
// defines removal of an absent image as success, so a nil error is not
// evidence that the image existed. The caller must already hold complete
// evidence that the image is unused; removal is never an observation.
func (c *Client) RemoveImage(ctx context.Context, image string) error {
	if err := validateID(image); err != nil {
		return fmt.Errorf("CRI RemoveImage: %w", err)
	}
	_, err := invoke(ctx, c, "RemoveImage", func(ctx context.Context) (*runtimeapi.RemoveImageResponse, error) {
		return c.images.RemoveImage(ctx, &runtimeapi.RemoveImageRequest{Image: &runtimeapi.ImageSpec{Image: image}})
	})
	return err
}

// ImageFsInfo reads the usage of every image filesystem the runtime reports.
// The response must name at least one image filesystem, each with a
// mountpoint, a timestamp, and used bytes. Container filesystems are not
// returned. The reported usage is the runtime's view of its image store; it
// is not a measurement of a devmapper thin pool or of the host disk.
func (c *Client) ImageFsInfo(ctx context.Context) ([]*runtimeapi.FilesystemUsage, error) {
	response, err := invoke(ctx, c, "ImageFsInfo", func(ctx context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
		return c.images.ImageFsInfo(ctx, &runtimeapi.ImageFsInfoRequest{})
	})
	if err != nil {
		return nil, err
	}
	filesystems := response.GetImageFilesystems()
	if len(filesystems) == 0 {
		return nil, errors.New("CRI ImageFsInfo: no image filesystem reported")
	}
	seen := make(map[string]bool)
	for _, usage := range filesystems {
		mountpoint := usage.GetFsId().GetMountpoint()
		if err := validateID(mountpoint); err != nil {
			return nil, fmt.Errorf("CRI ImageFsInfo: mountpoint: %w", err)
		}
		if seen[mountpoint] {
			return nil, errors.New("CRI ImageFsInfo: duplicate image filesystem")
		}
		seen[mountpoint] = true
		if usage.GetTimestamp() <= 0 || usage.GetUsedBytes() == nil {
			return nil, errors.New("CRI ImageFsInfo: image filesystem usage lacks a timestamp or used bytes")
		}
	}
	return filesystems, nil
}

// validateImage checks one listed image. CRI requires a positive size; tags
// and digests are retained as observations, not validated as references.
func validateImage(image *runtimeapi.Image) error {
	if err := validateID(image.GetId()); err != nil {
		return fmt.Errorf("image ID: %w", err)
	}
	if image.GetSize() == 0 {
		return errors.New("image size must be positive")
	}
	for _, reference := range image.GetRepoTags() {
		if err := validateID(reference); err != nil {
			return fmt.Errorf("image tag: %w", err)
		}
	}
	for _, reference := range image.GetRepoDigests() {
		if err := validateID(reference); err != nil {
			return fmt.Errorf("image digest: %w", err)
		}
	}
	return nil
}
