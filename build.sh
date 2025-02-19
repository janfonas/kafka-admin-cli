#!/bin/bash

# Exit on any error
set -e

# Get the current version from git tag if available
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")

# Build flags
BUILD_FLAGS=(
    "-o"
    "kac"
    "-ldflags"
    "-s -w -X main.version=${VERSION}"
)

echo "Building kac version ${VERSION}..."

# Run go mod tidy to ensure dependencies are up to date
go mod tidy

# Build the binary
CGO_ENABLED=0 go build "${BUILD_FLAGS[@]}"

echo "Build complete: $(pwd)/kac"
