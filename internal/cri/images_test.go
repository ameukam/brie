// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package cri

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type imageRuntime struct {
	runtimeapi.UnimplementedImageServiceServer
	mu      sync.Mutex
	images  []*runtimeapi.Image
	usage   []*runtimeapi.FilesystemUsage
	failure error
	removed []string
	filter  bool
}

func (s *imageRuntime) ListImages(_ context.Context, request *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filter = request.Filter != nil
	return &runtimeapi.ListImagesResponse{Images: s.images}, s.failure
}

func (s *imageRuntime) RemoveImage(_ context.Context, request *runtimeapi.RemoveImageRequest) (*runtimeapi.RemoveImageResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed = append(s.removed, request.GetImage().GetImage())
	return &runtimeapi.RemoveImageResponse{}, s.failure
}

func (s *imageRuntime) ImageFsInfo(context.Context, *runtimeapi.ImageFsInfoRequest) (*runtimeapi.ImageFsInfoResponse, error) {
	return &runtimeapi.ImageFsInfoResponse{ImageFilesystems: s.usage}, s.failure
}

func imageClient(t *testing.T, service *imageRuntime) *Client {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "cri.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	runtimeapi.RegisterImageServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return newClient(t, socket, time.Second)
}

func goodImage() *runtimeapi.Image {
	return &runtimeapi.Image{
		Id: "sha256:config", Size: 1024, Pinned: true,
		RepoTags:    []string{"registry.example/app:v1"},
		RepoDigests: []string{"registry.example/app@sha256:digest"},
	}
}

func goodUsage(mountpoint string) *runtimeapi.FilesystemUsage {
	return &runtimeapi.FilesystemUsage{
		Timestamp: 1, FsId: &runtimeapi.FilesystemIdentifier{Mountpoint: mountpoint},
		UsedBytes: &runtimeapi.UInt64Value{Value: 4096},
	}
}

func TestListImages(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		images func() []*runtimeapi.Image
		valid  bool
	}{
		{name: "empty", valid: true, images: func() []*runtimeapi.Image { return nil }},
		{name: "foreign and pinned", valid: true, images: func() []*runtimeapi.Image {
			untagged := goodImage()
			untagged.Id, untagged.RepoTags, untagged.RepoDigests, untagged.Pinned = "sha256:other", nil, nil, false
			return []*runtimeapi.Image{goodImage(), untagged}
		}},
		{name: "missing id", images: func() []*runtimeapi.Image {
			image := goodImage()
			image.Id = ""
			return []*runtimeapi.Image{image}
		}},
		{name: "invalid id", images: func() []*runtimeapi.Image {
			image := goodImage()
			image.Id = "\xff"
			return []*runtimeapi.Image{image}
		}},
		{name: "zero size", images: func() []*runtimeapi.Image {
			image := goodImage()
			image.Size = 0
			return []*runtimeapi.Image{image}
		}},
		{name: "invalid tag", images: func() []*runtimeapi.Image {
			image := goodImage()
			image.RepoTags = []string{""}
			return []*runtimeapi.Image{image}
		}},
		{name: "invalid digest", images: func() []*runtimeapi.Image {
			image := goodImage()
			image.RepoDigests = []string{"a\x00b"}
			return []*runtimeapi.Image{image}
		}},
		{name: "duplicate id", images: func() []*runtimeapi.Image {
			return []*runtimeapi.Image{goodImage(), goodImage()}
		}},
		{name: "one invalid among valid", images: func() []*runtimeapi.Image {
			invalid := goodImage()
			invalid.Id = "sha256:other"
			invalid.Size = 0
			return []*runtimeapi.Image{goodImage(), invalid}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			service := &imageRuntime{images: tt.images()}
			list, err := imageClient(t, service).ListImages(t.Context())
			service.mu.Lock()
			defer service.mu.Unlock()
			if service.filter {
				t.Fatal("image inventory must not be filtered")
			}
			if !tt.valid {
				if err == nil || list != nil {
					t.Fatalf("invalid image inventory = %v, %v", list, err)
				}
				return
			}
			if err != nil || len(list) != len(service.images) {
				t.Fatalf("image inventory = %v, %v", list, err)
			}
			for i := range list {
				if !proto.Equal(list[i], service.images[i]) {
					t.Fatalf("image inventory lost identity: %v", list[i])
				}
			}
		})
	}
}

func TestImageFsInfo(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		usage func() []*runtimeapi.FilesystemUsage
		valid bool
	}{
		{name: "one", valid: true, usage: func() []*runtimeapi.FilesystemUsage {
			return []*runtimeapi.FilesystemUsage{goodUsage("/var/lib/containerd")}
		}},
		{name: "two", valid: true, usage: func() []*runtimeapi.FilesystemUsage {
			return []*runtimeapi.FilesystemUsage{goodUsage("/a"), goodUsage("/b")}
		}},
		{name: "none", usage: func() []*runtimeapi.FilesystemUsage { return nil }},
		{name: "duplicate", usage: func() []*runtimeapi.FilesystemUsage {
			return []*runtimeapi.FilesystemUsage{goodUsage("/a"), goodUsage("/a")}
		}},
		{name: "missing mountpoint", usage: func() []*runtimeapi.FilesystemUsage {
			usage := goodUsage("")
			return []*runtimeapi.FilesystemUsage{usage}
		}},
		{name: "missing identifier", usage: func() []*runtimeapi.FilesystemUsage {
			usage := goodUsage("/a")
			usage.FsId = nil
			return []*runtimeapi.FilesystemUsage{usage}
		}},
		{name: "missing used bytes", usage: func() []*runtimeapi.FilesystemUsage {
			usage := goodUsage("/a")
			usage.UsedBytes = nil
			return []*runtimeapi.FilesystemUsage{usage}
		}},
		{name: "missing timestamp", usage: func() []*runtimeapi.FilesystemUsage {
			usage := goodUsage("/a")
			usage.Timestamp = 0
			return []*runtimeapi.FilesystemUsage{usage}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			service := &imageRuntime{usage: tt.usage()}
			usage, err := imageClient(t, service).ImageFsInfo(t.Context())
			if tt.valid != (err == nil) || !tt.valid && usage != nil || tt.valid && len(usage) != len(service.usage) {
				t.Fatalf("ImageFsInfo = %v, %v", usage, err)
			}
		})
	}
}

func TestImageRPCErrors(t *testing.T) {
	t.Parallel()
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.NotFound} {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			service := &imageRuntime{
				images: []*runtimeapi.Image{goodImage()}, usage: []*runtimeapi.FilesystemUsage{goodUsage("/a")},
				failure: status.Error(code, "runtime response"),
			}
			client := imageClient(t, service)
			if list, err := client.ListImages(t.Context()); status.Code(err) != code || list != nil {
				t.Fatalf("failed list returned partial results: %v, %v", list, err)
			}
			if usage, err := client.ImageFsInfo(t.Context()); status.Code(err) != code || usage != nil {
				t.Fatalf("failed usage returned partial results: %v, %v", usage, err)
			}
			err := client.RemoveImage(t.Context(), "sha256:config")
			if status.Code(err) != code || code == codes.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("RemoveImage error = %v, want %s", err, code)
			}
		})
	}
}

func TestRemoveImage(t *testing.T) {
	t.Parallel()
	service := &imageRuntime{}
	client := imageClient(t, service)
	if err := client.RemoveImage(t.Context(), "sha256:config"); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"", "\xff", "a\x00b"} {
		if err := client.RemoveImage(t.Context(), invalid); err == nil {
			t.Fatalf("invalid image reference %q accepted", invalid)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.RemoveImage(ctx, "sha256:other"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled removal reached the runtime: %v", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.removed) != 1 || service.removed[0] != "sha256:config" {
		t.Fatalf("removal requests = %v", service.removed)
	}
}

func FuzzValidateImage(f *testing.F) {
	f.Add("sha256:config", "registry.example/app:v1", "registry.example/app@sha256:digest", uint64(1))
	f.Add("", "", "", uint64(0))
	f.Add("\xff", "a\x00", "b", uint64(7))
	f.Fuzz(func(t *testing.T, id, tag, digest string, size uint64) {
		image := &runtimeapi.Image{Id: id, RepoTags: []string{tag}, RepoDigests: []string{digest}, Size: size}
		if validateImage(image) != nil {
			return
		}
		for _, value := range []string{id, tag, digest} {
			if validateID(value) != nil {
				t.Fatalf("accepted invalid image field %q", value)
			}
		}
		if size == 0 {
			t.Fatal("accepted zero image size")
		}
	})
}
