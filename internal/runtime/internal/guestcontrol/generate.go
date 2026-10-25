// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package guestcontrol

//go:generate protoc --proto_path=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative guestcontrolpb/guest_control.proto
