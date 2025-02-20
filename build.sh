#!/bin/bash

# Parse command line arguments
SKIP_TESTS=false
while [[ $# -gt 0 ]]; do
    case $1 in
        -skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run tests unless skipped
if [ "$SKIP_TESTS" = false ]; then
    echo "Running tests..."
    go test ./... || exit 1
fi

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
go build \
  -ldflags="-s -w \
  -X 'github.com/janfonas/kafka-admin-cli/cmd.version=${VERSION}' \
  -X 'github.com/janfonas/kafka-admin-cli/cmd.commit=${GIT_COMMIT}' \
  -X 'github.com/janfonas/kafka-admin-cli/cmd.buildDate=${BUILD_DATE}'" \
  -o kac

echo "Build complete: $(pwd)/kac"
