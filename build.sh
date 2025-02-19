#!/bin/bash

# Exit on any error
set -e

# Get version information
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE=$(date -u '+%Y-%m-%d %H:%M:%S')

echo "Building kac version ${VERSION}..."
echo "Git commit: ${GIT_COMMIT}"
echo "Build date: ${BUILD_DATE}"

# Run go mod tidy to ensure dependencies are up to date
go mod tidy

# Build the binary
CGO_ENABLED=0 go install -ldflags "-s -w -X github.com/janfonas/kafka-admin-cli/cmd.version=${VERSION} -X github.com/janfonas/kafka-admin-cli/cmd.gitCommit=${GIT_COMMIT} -X github.com/janfonas/kafka-admin-cli/cmd.buildDate=${BUILD_DATE}" && cp $(go env GOPATH)/bin/kafka-admin-cli ./kac

echo "Build complete: $(pwd)/kac"
