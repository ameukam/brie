// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package checkpoint defines the committed checkpoint manifest of DESIGN 5.1,
// its storage, and the conditional commit of a Sandbox checkpoint reference
// (DESIGN 5.2). A valid manifest describes artifacts; it neither proves that
// their bytes are durable nor authorizes a restore.
package checkpoint

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ameukam/brie/internal/lifecycleid"
)

// ManifestSchemaVersion identifies this manifest schema.
const ManifestSchemaVersion = "brie.checkpoint/v1alpha1"

// Bounds on an encoded manifest and its lists.
const (
	MaxManifestBytes = 64 << 10
	MaxDisks         = 16
	MaxDevices       = 32
	MaxImageDigests  = 16
	maxTextBytes     = 128
	maxReferenceLen  = 1024
)

// Manifest identifies one committed checkpoint. Lists are sorted so that
// every manifest has exactly one encoding.
type Manifest struct {
	SchemaVersion         string        `json:"schema_version"`
	SandboxUID            string        `json:"sandbox_uid"`
	SourcePodUID          string        `json:"source_pod_uid"`
	PlacementEpoch        int64         `json:"placement_epoch"`
	OperationID           string        `json:"operation_id"`
	CheckpointGeneration  int64         `json:"checkpoint_generation"`
	Artifacts             Artifacts     `json:"artifacts"`
	Compatibility         Compatibility `json:"compatibility"`
	DeviceLayout          []Device      `json:"device_layout"`
	ResumeProtocolVersion string        `json:"resume_protocol_version"`
	SafePoint             SafePoint     `json:"safe_point"`
	CreatedAt             time.Time     `json:"created_at"`
	EncryptionKeyRef      string        `json:"encryption_key_ref"`
	AccessPolicyRef       string        `json:"access_policy_ref"`
	RetentionPolicyRef    string        `json:"retention_policy_ref"`
}

// Artifacts lists the snapshot files and every disk generation.
type Artifacts struct {
	VMState Artifact       `json:"vm_state"`
	Memory  Artifact       `json:"memory"`
	Disks   []DiskArtifact `json:"disks"`
}

// ObjectRef names one immutable object generation.
type ObjectRef struct {
	Bucket     string `json:"bucket"`
	Name       string `json:"name"`
	Generation int64  `json:"generation"`
}

// Artifact identifies stored bytes by object generation, size, and digests.
// CRC32C is the base64 big-endian form that the object store reports, so a
// stored generation can be rechecked without downloading it.
type Artifact struct {
	Object    ObjectRef `json:"object"`
	SizeBytes int64     `json:"size_bytes"`
	SHA256    string    `json:"sha256"`
	CRC32C    string    `json:"crc32c"`
}

// DiskArtifact is the captured generation of one block device.
type DiskArtifact struct {
	Device   string   `json:"device"`
	Artifact Artifact `json:"artifact"`
}

// DeviceKind names a device class in the VM layout.
type DeviceKind string

// Device kinds a restore must recreate.
const (
	DeviceBlock   DeviceKind = "block"
	DeviceNetwork DeviceKind = "network"
	DeviceVsock   DeviceKind = "vsock"
)

// Device is one entry of the VM device layout. LogicalVolume identifies a
// block device's volume independently of any source host path.
type Device struct {
	Name           string     `json:"name"`
	Kind           DeviceKind `json:"kind"`
	LogicalVolume  string     `json:"logical_volume,omitempty"`
	GuestMountPath string     `json:"guest_mount_path,omitempty"`
	ReadOnly       bool       `json:"read_only,omitempty"`
}

// Compatibility is the restore compatibility tuple of DESIGN 3.3 and 9.3.
// CPUTemplate is "none" when no template is applied. GuestAgentVersion is the
// in-guest runtime agent's version and SecurityProfile names the qualified
// host and guest security profile; both are required because a different
// agent or security configuration is a different compatibility class.
type Compatibility struct {
	HostKernel            string   `json:"host_kernel"`
	CPUModel              string   `json:"cpu_model"`
	GICVersion            string   `json:"gic_version"`
	FirecrackerVersion    string   `json:"firecracker_version"`
	SnapshotFormatVersion string   `json:"snapshot_format_version"`
	KataVersion           string   `json:"kata_version"`
	GuestAgentVersion     string   `json:"guest_agent_version"`
	SecurityProfile       string   `json:"security_profile"`
	GuestKernel           string   `json:"guest_kernel"`
	CPUTemplate           string   `json:"cpu_template"`
	ImageDigests          []string `json:"image_digests"`
	VCPUCount             int32    `json:"vcpu_count"`
	MemoryMiB             int64    `json:"memory_mib"`
}

// SafePoint is the application state the checkpoint captured.
type SafePoint struct {
	ID                      string `json:"id"`
	ExternalEffectWatermark uint64 `json:"external_effect_watermark"`
}

// Validate checks every manifest field and the cross-references between
// disks and the device layout.
func (m Manifest) Validate() error {
	if m.SchemaVersion != ManifestSchemaVersion {
		return fmt.Errorf("unsupported manifest schema version %q", m.SchemaVersion)
	}
	if err := lifecycleid.UID("sandbox_uid", m.SandboxUID); err != nil {
		return err
	}
	if err := lifecycleid.UID("source_pod_uid", m.SourcePodUID); err != nil {
		return err
	}
	if err := lifecycleid.Epoch("placement_epoch", m.PlacementEpoch); err != nil {
		return err
	}
	if err := lifecycleid.Token("operation_id", m.OperationID); err != nil {
		return err
	}
	if m.CheckpointGeneration <= 0 {
		return errors.New("checkpoint_generation must be positive")
	}
	if err := m.Compatibility.validate(); err != nil {
		return err
	}
	blockDevices, err := validateDeviceLayout(m.DeviceLayout)
	if err != nil {
		return err
	}
	if err := m.Artifacts.validate(blockDevices); err != nil {
		return err
	}
	if err := lifecycleid.Text("resume_protocol_version", m.ResumeProtocolVersion, maxTextBytes); err != nil {
		return err
	}
	if err := lifecycleid.Token("safe_point.id", m.SafePoint.ID); err != nil {
		return err
	}
	if m.CreatedAt.IsZero() || m.CreatedAt.Location() != time.UTC {
		return errors.New("created_at must be a UTC time")
	}
	for _, ref := range []struct{ field, value string }{
		{"encryption_key_ref", m.EncryptionKeyRef},
		{"access_policy_ref", m.AccessPolicyRef},
		{"retention_policy_ref", m.RetentionPolicyRef},
	} {
		if err := lifecycleid.Text(ref.field, ref.value, maxReferenceLen); err != nil {
			return err
		}
	}
	return nil
}

func (c Compatibility) validate() error {
	for _, field := range []struct{ name, value string }{
		{"host_kernel", c.HostKernel},
		{"cpu_model", c.CPUModel},
		{"gic_version", c.GICVersion},
		{"firecracker_version", c.FirecrackerVersion},
		{"snapshot_format_version", c.SnapshotFormatVersion},
		{"kata_version", c.KataVersion},
		{"guest_agent_version", c.GuestAgentVersion},
		{"security_profile", c.SecurityProfile},
		{"guest_kernel", c.GuestKernel},
		{"cpu_template", c.CPUTemplate},
	} {
		if err := lifecycleid.Text("compatibility."+field.name, field.value, maxTextBytes); err != nil {
			return err
		}
	}
	if len(c.ImageDigests) == 0 || len(c.ImageDigests) > MaxImageDigests {
		return fmt.Errorf("compatibility.image_digests must list 1 to %d digests", MaxImageDigests)
	}
	for i, digest := range c.ImageDigests {
		hexDigits, ok := strings.CutPrefix(digest, "sha256:")
		if !ok || lifecycleid.LowerHex("image digest", hexDigits, 64) != nil {
			return errors.New("compatibility.image_digests entries must be sha256:<64 lowercase hex digits>")
		}
		if i > 0 && c.ImageDigests[i-1] >= digest {
			return errors.New("compatibility.image_digests must be sorted without duplicates")
		}
	}
	if c.VCPUCount <= 0 || c.MemoryMiB <= 0 {
		return errors.New("compatibility.vcpu_count and memory_mib must be positive")
	}
	return nil
}

// validateDeviceLayout returns the block device names.
func validateDeviceLayout(devices []Device) (map[string]bool, error) {
	if len(devices) == 0 || len(devices) > MaxDevices {
		return nil, fmt.Errorf("device_layout must list 1 to %d devices", MaxDevices)
	}
	blocks := make(map[string]bool)
	for i, device := range devices {
		if err := lifecycleid.Token("device_layout.name", device.Name); err != nil {
			return nil, err
		}
		if i > 0 && devices[i-1].Name >= device.Name {
			return nil, errors.New("device_layout must be sorted by name without duplicates")
		}
		switch device.Kind {
		case DeviceBlock:
			if err := lifecycleid.Token("device_layout.logical_volume", device.LogicalVolume); err != nil {
				return nil, err
			}
			if device.GuestMountPath != "" && !validGuestPath(device.GuestMountPath) {
				return nil, errors.New("device_layout.guest_mount_path must be a clean absolute path")
			}
			blocks[device.Name] = true
		case DeviceNetwork, DeviceVsock:
			if device.LogicalVolume != "" || device.GuestMountPath != "" || device.ReadOnly {
				return nil, fmt.Errorf("%s device %s must not carry volume fields", device.Kind, device.Name)
			}
		default:
			return nil, fmt.Errorf("device_layout.kind %q is not supported", device.Kind)
		}
	}
	return blocks, nil
}

func validGuestPath(value string) bool {
	return len(value) <= maxReferenceLen && path.IsAbs(value) && path.Clean(value) == value &&
		lifecycleid.Text("path", value, maxReferenceLen) == nil
}

func (a Artifacts) validate(blockDevices map[string]bool) error {
	if err := a.VMState.validate("artifacts.vm_state"); err != nil {
		return err
	}
	if err := a.Memory.validate("artifacts.memory"); err != nil {
		return err
	}
	if len(a.Disks) == 0 || len(a.Disks) > MaxDisks {
		return fmt.Errorf("artifacts.disks must list 1 to %d disks", MaxDisks)
	}
	if len(a.Disks) != len(blockDevices) {
		return errors.New("artifacts.disks must capture every block device in device_layout")
	}
	objects := map[[2]string]bool{
		{a.VMState.Object.Bucket, a.VMState.Object.Name}: true,
	}
	if objects[[2]string{a.Memory.Object.Bucket, a.Memory.Object.Name}] {
		return errors.New("artifacts must reference distinct objects")
	}
	objects[[2]string{a.Memory.Object.Bucket, a.Memory.Object.Name}] = true
	for i, disk := range a.Disks {
		if !blockDevices[disk.Device] {
			return fmt.Errorf("disk device %q is not a block device in device_layout", disk.Device)
		}
		if i > 0 && a.Disks[i-1].Device >= disk.Device {
			return errors.New("artifacts.disks must be sorted by device without duplicates")
		}
		if err := disk.Artifact.validate("artifacts.disks." + disk.Device); err != nil {
			return err
		}
		key := [2]string{disk.Artifact.Object.Bucket, disk.Artifact.Object.Name}
		if objects[key] {
			return errors.New("artifacts must reference distinct objects")
		}
		objects[key] = true
	}
	return nil
}

func (a Artifact) validate(field string) error {
	if err := a.Object.Validate(); err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	if a.SizeBytes <= 0 {
		return fmt.Errorf("%s.size_bytes must be positive", field)
	}
	if err := lifecycleid.LowerHex(field+".sha256", a.SHA256, 64); err != nil {
		return err
	}
	if !ValidCRC32C(a.CRC32C) {
		return fmt.Errorf("%s.crc32c must be base64 of four bytes", field)
	}
	return nil
}

// Validate checks a bucket name, an object name, and a positive generation.
func (o ObjectRef) Validate() error {
	if err := ValidateBucket(o.Bucket); err != nil {
		return err
	}
	if err := ValidateObjectName(o.Name); err != nil {
		return err
	}
	if o.Generation <= 0 {
		return errors.New("object generation must be positive")
	}
	return nil
}

// ValidateBucket accepts 3 to 63 lowercase letters, digits, '-', '_', and
// '.', starting and ending with a letter or digit.
func ValidateBucket(bucket string) error {
	if len(bucket) < 3 || len(bucket) > 63 {
		return errors.New("bucket name must be 3 to 63 bytes")
	}
	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		alphanumeric := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if (i == 0 || i == len(bucket)-1) && !alphanumeric {
			return errors.New("bucket name must start and end with a lowercase letter or digit")
		}
		if !alphanumeric && c != '-' && c != '_' && c != '.' {
			return errors.New("bucket name may contain only lowercase letters, digits, '-', '_', and '.'")
		}
	}
	return nil
}

// ValidateObjectName accepts 1 to 1024 bytes of UTF-8 without control
// characters, excluding "." and "..".
func ValidateObjectName(name string) error {
	if name == "" || len(name) > 1024 || !utf8.ValidString(name) || name == "." || name == ".." {
		return errors.New("object name must be 1 to 1024 bytes of UTF-8 and must not be \".\" or \"..\"")
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return errors.New("object name must not contain control characters")
		}
	}
	if strings.HasPrefix(name, ".well-known/acme-challenge/") {
		return errors.New("object name must not use the reserved .well-known/acme-challenge/ prefix")
	}
	return nil
}

// ValidCRC32C reports whether value is the base64 form of a CRC32C checksum.
func ValidCRC32C(value string) bool {
	decoded, err := base64.StdEncoding.Strict().DecodeString(value)
	return err == nil && len(decoded) == 4
}

// EncodeManifest validates and canonically encodes a manifest.
func EncodeManifest(m Manifest) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}
	if len(data) > MaxManifestBytes {
		return nil, fmt.Errorf("manifest exceeds %d bytes", MaxManifestBytes)
	}
	return data, nil
}

// DecodeManifest accepts only the canonical encoding of a valid manifest:
// unknown fields, duplicate keys, trailing data, and any input that differs
// from its re-encoding are rejected.
func DecodeManifest(data []byte) (Manifest, error) {
	if len(data) > MaxManifestBytes {
		return Manifest{}, fmt.Errorf("manifest exceeds %d bytes", MaxManifestBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var m Manifest
	if err := decoder.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Manifest{}, errors.New("decode manifest: trailing data")
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, fmt.Errorf("invalid manifest: %w", err)
	}
	canonical, err := json.Marshal(m)
	if err != nil {
		return Manifest{}, fmt.Errorf("encode manifest: %w", err)
	}
	if !bytes.Equal(canonical, data) {
		return Manifest{}, errors.New("manifest is not canonically encoded")
	}
	return m, nil
}

// ArtifactList returns the VM state, memory, and disk artifacts in manifest
// order.
func (m Manifest) ArtifactList() []Artifact {
	list := []Artifact{m.Artifacts.VMState, m.Artifacts.Memory}
	for _, disk := range m.Artifacts.Disks {
		list = append(list, disk.Artifact)
	}
	return list
}
