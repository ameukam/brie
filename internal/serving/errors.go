// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package serving

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"strconv"

	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// errorCode reduces err to a bounded code for logs. Runtime, streaming proxy,
// and API server errors can echo commands, streaming tokens, or credentials,
// and agentlog writes error text verbatim, so their text is never logged.
func errorCode(err error) string {
	var apiStatus apierrors.APIStatus
	var pathError *fs.PathError
	var netError net.Error
	switch {
	case errors.Is(err, context.Canceled):
		return "Canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "DeadlineExceeded"
	case errors.As(err, &apiStatus):
		return "HTTP" + strconv.Itoa(int(apiStatus.Status().Code))
	case errors.As(err, &pathError):
		return "FileError"
	case errors.As(err, &netError):
		return "NetworkError"
	}
	if grpcStatus, ok := status.FromError(err); ok {
		return grpcStatus.Code().String()
	}
	return "Unknown"
}

// failureAttrs describes an authentication or authorization failure: the
// fixed text of a local outcome, or the review name and a bounded code.
func failureAttrs(err error) []slog.Attr {
	for _, known := range []error{errNoCredentials, errTokenRejected, errTokenAudience, errNoCommonName, errAccessDenied} {
		if errors.Is(err, known) {
			return []slog.Attr{slog.String("reason", known.Error())}
		}
	}
	var review *reviewError
	if errors.As(err, &review) {
		return []slog.Attr{slog.String("reason", review.review+" failed"), slog.String("code", errorCode(review.err))}
	}
	return []slog.Attr{slog.String("reason", "unknown"), slog.String("code", errorCode(err))}
}
