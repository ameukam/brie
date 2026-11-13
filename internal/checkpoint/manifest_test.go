// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package checkpoint

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

const (
	testSandboxUID = "0f8fad5b-d9cb-469f-a165-70867728950e"
	testPodUID     = "7c9e6679-7425-40de-944b-e07fc1f90ae7"
	testSHA256     = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	testCRC32C     = "AAAAAA=="
)

func testArtifact(name string) Artifact {
	return Artifact{
		Object:    ObjectRef{Bucket: "brie-checkpoints", Name: "sandboxes/s1/op-1/" + name, Generation: 1700000000000001},
		SizeBytes: 4096, SHA256: testSHA256, CRC32C: testCRC32C,
	}
}

func validManifest() Manifest {
	return Manifest{
		SchemaVersion: ManifestSchemaVersion, SandboxUID: testSandboxUID, SourcePodUID: testPodUID,
		PlacementEpoch: 3, OperationID: "op-1", CheckpointGeneration: 2,
		Artifacts: Artifacts{
			VMState: testArtifact("vm-state"),
			Memory:  testArtifact("memory"),
			Disks: []DiskArtifact{
				{Device: "data", Artifact: testArtifact("disk-data")},
				{Device: "rootfs", Artifact: testArtifact("disk-rootfs")},
			},
		},
		Compatibility: Compatibility{
			HostKernel: "6.8.0-1015-gcp", CPUModel: "Google Axion", GICVersion: "v3",
			FirecrackerVersion: "1.17.0", SnapshotFormatVersion: "8.0.0", KataVersion: "4.3.1",
			GuestKernel: "6.18.56", CPUTemplate: "none",
			ImageDigests: []string{"sha256:" + testSHA256},
			VCPUCount:    2, MemoryMiB: 2048,
		},
		DeviceLayout: []Device{
			{Name: "data", Kind: DeviceBlock, LogicalVolume: "workspace", GuestMountPath: "/data"},
			{Name: "eth0", Kind: DeviceNetwork},
			{Name: "rootfs", Kind: DeviceBlock, LogicalVolume: "container-rootfs"},
			{Name: "vsock", Kind: DeviceVsock},
		},
		ResumeProtocolVersion: "brie.guestsupervisor/v1",
		SafePoint:             SafePoint{ID: "counter-3", ExternalEffectWatermark: 3},
		CreatedAt:             time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC),
		EncryptionKeyRef:      "projects/p/locations/l/keyRings/r/cryptoKeys/k",
		AccessPolicyRef:       "brie-checkpoint-readers",
		RetentionPolicyRef:    "retain-last-2",
	}
}

func TestManifestRoundTrip(t *testing.T) {
	encoded, err := EncodeManifest(validManifest())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	again, err := EncodeManifest(decoded)
	if err != nil || !bytes.Equal(again, encoded) {
		t.Fatalf("re-encoding differs: %v", err)
	}
	if got := len(decoded.ArtifactList()); got != 4 {
		t.Fatalf("ArtifactList length = %d, want 4", got)
	}
}

func TestManifestValidate(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Manifest)
		want   string
	}{
		{name: "schema", mutate: func(m *Manifest) { m.SchemaVersion = "v0" }, want: "schema version"},
		{name: "sandbox UID", mutate: func(m *Manifest) { m.SandboxUID = "s" }, want: "sandbox_uid"},
		{name: "pod UID", mutate: func(m *Manifest) { m.SourcePodUID = "" }, want: "source_pod_uid"},
		{name: "epoch", mutate: func(m *Manifest) { m.PlacementEpoch = 0 }, want: "placement_epoch"},
		{name: "operation", mutate: func(m *Manifest) { m.OperationID = "a/b" }, want: "operation_id"},
		{name: "generation", mutate: func(m *Manifest) { m.CheckpointGeneration = -1 }, want: "checkpoint_generation"},
		{name: "host kernel", mutate: func(m *Manifest) { m.Compatibility.HostKernel = "" }, want: "host_kernel"},
		{name: "cpu model", mutate: func(m *Manifest) { m.Compatibility.CPUModel = "\n" }, want: "cpu_model"},
		{name: "gic", mutate: func(m *Manifest) { m.Compatibility.GICVersion = "" }, want: "gic_version"},
		{name: "firecracker", mutate: func(m *Manifest) { m.Compatibility.FirecrackerVersion = "" }, want: "firecracker_version"},
		{name: "snapshot format", mutate: func(m *Manifest) { m.Compatibility.SnapshotFormatVersion = "" }, want: "snapshot_format_version"},
		{name: "kata", mutate: func(m *Manifest) { m.Compatibility.KataVersion = "" }, want: "kata_version"},
		{name: "guest kernel", mutate: func(m *Manifest) { m.Compatibility.GuestKernel = "" }, want: "guest_kernel"},
		{name: "cpu template", mutate: func(m *Manifest) { m.Compatibility.CPUTemplate = "" }, want: "cpu_template"},
		{name: "no images", mutate: func(m *Manifest) { m.Compatibility.ImageDigests = nil }, want: "image_digests must list"},
		{name: "tag image", mutate: func(m *Manifest) { m.Compatibility.ImageDigests = []string{"latest"} }, want: "sha256:"},
		{name: "unsorted images", mutate: func(m *Manifest) {
			m.Compatibility.ImageDigests = []string{"sha256:" + strings.Repeat("b", 64), "sha256:" + strings.Repeat("a", 64)}
		}, want: "sorted"},
		{name: "vcpu", mutate: func(m *Manifest) { m.Compatibility.VCPUCount = 0 }, want: "vcpu_count"},
		{name: "memory", mutate: func(m *Manifest) { m.Compatibility.MemoryMiB = 0 }, want: "memory_mib"},
		{name: "no devices", mutate: func(m *Manifest) { m.DeviceLayout = nil }, want: "device_layout must list"},
		{name: "device name", mutate: func(m *Manifest) { m.DeviceLayout[1].Name = "" }, want: "device_layout.name"},
		{name: "unsorted devices", mutate: func(m *Manifest) {
			m.DeviceLayout[0], m.DeviceLayout[1] = m.DeviceLayout[1], m.DeviceLayout[0]
		}, want: "sorted by name"},
		{name: "device kind", mutate: func(m *Manifest) { m.DeviceLayout[1].Kind = "gpu" }, want: "not supported"},
		{name: "block without volume", mutate: func(m *Manifest) { m.DeviceLayout[0].LogicalVolume = "" }, want: "logical_volume"},
		{name: "relative mount", mutate: func(m *Manifest) { m.DeviceLayout[0].GuestMountPath = "data" }, want: "guest_mount_path"},
		{name: "unclean mount", mutate: func(m *Manifest) { m.DeviceLayout[0].GuestMountPath = "/data/../etc" }, want: "guest_mount_path"},
		{name: "network with volume", mutate: func(m *Manifest) { m.DeviceLayout[1].LogicalVolume = "v" }, want: "must not carry volume"},
		{name: "vm state object", mutate: func(m *Manifest) { m.Artifacts.VMState.Object.Generation = 0 }, want: "artifacts.vm_state"},
		{name: "memory size", mutate: func(m *Manifest) { m.Artifacts.Memory.SizeBytes = 0 }, want: "artifacts.memory.size_bytes"},
		{name: "memory digest", mutate: func(m *Manifest) { m.Artifacts.Memory.SHA256 = strings.ToUpper(testSHA256) }, want: "sha256"},
		{name: "memory crc", mutate: func(m *Manifest) { m.Artifacts.Memory.CRC32C = "AAAA" }, want: "crc32c"},
		{name: "no disks", mutate: func(m *Manifest) { m.Artifacts.Disks = nil }, want: "artifacts.disks must list"},
		{name: "missing disk", mutate: func(m *Manifest) { m.Artifacts.Disks = m.Artifacts.Disks[:1] }, want: "every block device"},
		{name: "disk for network device", mutate: func(m *Manifest) { m.Artifacts.Disks[0].Device = "eth0" }, want: "not a block device"},
		{name: "unsorted disks", mutate: func(m *Manifest) {
			m.Artifacts.Disks[0], m.Artifacts.Disks[1] = m.Artifacts.Disks[1], m.Artifacts.Disks[0]
		}, want: "sorted by device"},
		{name: "disk artifact", mutate: func(m *Manifest) { m.Artifacts.Disks[1].Artifact.Object.Bucket = "B" }, want: "artifacts.disks.rootfs"},
		{name: "shared memory object", mutate: func(m *Manifest) { m.Artifacts.Memory.Object = m.Artifacts.VMState.Object }, want: "distinct objects"},
		{name: "shared disk object", mutate: func(m *Manifest) { m.Artifacts.Disks[1].Artifact.Object = m.Artifacts.Memory.Object }, want: "distinct objects"},
		{name: "resume protocol", mutate: func(m *Manifest) { m.ResumeProtocolVersion = "" }, want: "resume_protocol_version"},
		{name: "safe point", mutate: func(m *Manifest) { m.SafePoint.ID = "" }, want: "safe_point.id"},
		{name: "created zero", mutate: func(m *Manifest) { m.CreatedAt = time.Time{} }, want: "created_at"},
		{name: "created offset", mutate: func(m *Manifest) {
			m.CreatedAt = m.CreatedAt.In(time.FixedZone("x", 3600))
		}, want: "created_at"},
		{name: "encryption", mutate: func(m *Manifest) { m.EncryptionKeyRef = "" }, want: "encryption_key_ref"},
		{name: "access", mutate: func(m *Manifest) { m.AccessPolicyRef = "" }, want: "access_policy_ref"},
		{name: "retention", mutate: func(m *Manifest) { m.RetentionPolicyRef = strings.Repeat("r", 1025) }, want: "retention_policy_ref"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := validManifest()
			tt.mutate(&m)
			err := m.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error = %v, want %q", err, tt.want)
			}
			if _, err := EncodeManifest(m); err == nil {
				t.Fatal("EncodeManifest accepted an invalid manifest")
			}
		})
	}
}

func TestObjectNames(t *testing.T) {
	for _, tt := range []struct {
		bucket string
		ok     bool
	}{
		{"abc", true}, {"brie_ckpt.v1-a", true}, {"ab", false}, {strings.Repeat("a", 64), false},
		{"-abc", false}, {"abc-", false}, {"aBc", false}, {"a b", false},
	} {
		if err := ValidateBucket(tt.bucket); (err == nil) != tt.ok {
			t.Errorf("ValidateBucket(%q) = %v, want ok %v", tt.bucket, err, tt.ok)
		}
	}
	for _, tt := range []struct {
		name string
		ok   bool
	}{
		{"a/b/c", true}, {"é", true}, {"", false}, {".", false}, {"..", false},
		{strings.Repeat("a", 1025), false}, {"a\nb", false}, {"a\x7f", false}, {"a\u0085", false},
		{"\xff", false}, {".well-known/acme-challenge/x", false},
	} {
		if err := ValidateObjectName(tt.name); (err == nil) != tt.ok {
			t.Errorf("ValidateObjectName(%q) = %v, want ok %v", tt.name, err, tt.ok)
		}
	}
	if (ObjectRef{Bucket: "abc", Name: "", Generation: 1}).Validate() == nil {
		t.Error("ObjectRef accepted an empty name")
	}
}

func TestDecodeManifestRejects(t *testing.T) {
	encoded, err := EncodeManifest(validManifest())
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, tt := range []struct {
		name string
		data string
		want string
	}{
		{name: "empty", data: "", want: "decode manifest"},
		{name: "oversized", data: strings.Repeat(" ", MaxManifestBytes+1), want: "exceeds"},
		{name: "unknown field", data: strings.Replace(text, `"schema_version"`, `"x":1,"schema_version"`, 1), want: "unknown field"},
		{name: "trailing data", data: text + "{}", want: "trailing data"},
		{name: "trailing newline", data: text + "\n", want: "not canonically encoded"},
		{name: "duplicate key", data: strings.Replace(text, `"placement_epoch":3`, `"placement_epoch":4,"placement_epoch":3`, 1), want: "not canonically encoded"},
		{name: "invalid field", data: strings.Replace(text, `"placement_epoch":3`, `"placement_epoch":0`, 1), want: "invalid manifest"},
		{name: "string epoch", data: strings.Replace(text, `"placement_epoch":3`, `"placement_epoch":"3"`, 1), want: "decode manifest"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeManifest([]byte(tt.data))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("DecodeManifest error = %v, want %q", err, tt.want)
			}
		})
	}
	swapped := strings.Replace(text, `"operation_id":"op-1","checkpoint_generation":2`, `"checkpoint_generation":2,"operation_id":"op-1"`, 1)
	if swapped == text {
		t.Fatal("test fixture did not reorder fields")
	}
	if _, err := DecodeManifest([]byte(swapped)); err == nil || !strings.Contains(err.Error(), "not canonically encoded") {
		t.Fatalf("reordered manifest error = %v", err)
	}
}

func TestEncodeManifestNearFieldBounds(t *testing.T) {
	m := validManifest()
	m.EncryptionKeyRef = strings.Repeat("k", maxReferenceLen)
	m.AccessPolicyRef = strings.Repeat("a", maxReferenceLen)
	m.RetentionPolicyRef = strings.Repeat("r", maxReferenceLen)
	for i := range m.Artifacts.Disks {
		m.Artifacts.Disks[i].Artifact.Object.Name = strings.Repeat("d", 1000) + m.Artifacts.Disks[i].Device
	}
	m.Artifacts.VMState.Object.Name = strings.Repeat("v", 1024)
	m.Artifacts.Memory.Object.Name = strings.Repeat("m", 1024)
	if _, err := EncodeManifest(m); err != nil {
		t.Fatalf("large valid manifest rejected: %v", err)
	}
}

func FuzzDecodeManifest(f *testing.F) {
	encoded, err := EncodeManifest(validManifest())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte(`{"schema_version":"brie.checkpoint/v1alpha1"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeManifest(data)
		if err != nil {
			return
		}
		if err := m.Validate(); err != nil {
			t.Fatalf("decoded manifest is invalid: %v", err)
		}
		again, err := EncodeManifest(m)
		if err != nil || !bytes.Equal(again, data) {
			t.Fatalf("accepted non-canonical manifest %q: %v", data, err)
		}
	})
}
