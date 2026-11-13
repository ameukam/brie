// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

// Package lifecycleid validates the identifiers that lifecycle operations,
// the guest supervisor protocol, and checkpoint manifests share (DESIGN 4.2).
// Validation checks syntax only; it does not authorize an identity.
package lifecycleid

import "fmt"

// MaxTokenBytes bounds operation and safe-point identifiers so they fit in
// object names and protocol messages.
const MaxTokenBytes = 128

// UID requires the lowercase RFC 4122 text form that the Kubernetes API
// server assigns to object UIDs.
func UID(field, value string) error {
	if len(value) != 36 {
		return fmt.Errorf("%s must be a lowercase UUID", field)
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return fmt.Errorf("%s must be a lowercase UUID", field)
			}
		default:
			if !isLowerHex(c) {
				return fmt.Errorf("%s must be a lowercase UUID", field)
			}
		}
	}
	return nil
}

// Token requires 1 to MaxTokenBytes bytes of ASCII letters, digits, '.',
// '_', ':', or '-', starting with a letter or digit. Operation IDs and
// safe-point IDs use this form so they never need escaping.
func Token(field, value string) error {
	if value == "" || len(value) > MaxTokenBytes {
		return fmt.Errorf("%s must be 1 to %d bytes", field, MaxTokenBytes)
	}
	if !isAlphanumeric(value[0]) {
		return fmt.Errorf("%s must start with a letter or digit", field)
	}
	for i := 1; i < len(value); i++ {
		c := value[i]
		if !isAlphanumeric(c) && c != '.' && c != '_' && c != ':' && c != '-' {
			return fmt.Errorf("%s contains a byte outside [A-Za-z0-9._:-]", field)
		}
	}
	return nil
}

// LowerHex requires exactly length lowercase hexadecimal digits, the form used
// for SHA-256 digests and entropy nonces.
func LowerHex(field, value string, length int) error {
	if len(value) != length {
		return fmt.Errorf("%s must be %d lowercase hexadecimal digits", field, length)
	}
	for i := 0; i < len(value); i++ {
		if !isLowerHex(value[i]) {
			return fmt.Errorf("%s must be %d lowercase hexadecimal digits", field, length)
		}
	}
	return nil
}

// Epoch requires a positive placement epoch.
func Epoch(field string, value int64) error {
	if value <= 0 {
		return fmt.Errorf("%s must be positive", field)
	}
	return nil
}

// Text requires 1 to maxBytes bytes of printable ASCII, which keeps
// operator-visible reasons free of control characters and terminal escapes.
func Text(field, value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes {
		return fmt.Errorf("%s must be 1 to %d bytes of printable ASCII", field, maxBytes)
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return fmt.Errorf("%s must be 1 to %d bytes of printable ASCII", field, maxBytes)
		}
	}
	return nil
}

func isLowerHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

func isAlphanumeric(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
