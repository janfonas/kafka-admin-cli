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

# Build binary
echo "Building binary..."
go build -o kac
echo "Build complete: ./kac"
