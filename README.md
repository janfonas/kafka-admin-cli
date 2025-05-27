# Kafka Admin CLI (kac)

A command-line interface for Apache Kafka administration, built with the Franz-go client library. Designed as a management companion to [kcat](https://github.com/edenhill/kcat), providing comprehensive tools for Kafka cluster administration.

## Overview

`kac` provides a streamlined interface for managing:
- Kafka Topics
- Access Control Lists (ACLs)
- Consumer Groups

Built with security in mind, supporting SASL authentication and TLS encryption.

## Quick Start

### Installation

#### Pre-compiled Binaries (Recommended)
Download the latest pre-compiled binary for your platform from the [GitHub Releases](https://github.com/janfonas/kafka-admin-cli/releases) page.

```bash
# Linux (x86_64)
curl -L https://github.com/janfonas/kafka-admin-cli/releases/latest/download/kafka-admin-cli_Linux_x86_64.tar.gz | tar xz
sudo mv kac /usr/local/bin/

# macOS (Apple Silicon)
curl -L https://github.com/janfonas/kafka-admin-cli/releases/latest/download/kafka-admin-cli_Darwin_arm64.tar.gz | tar xz
sudo mv kac /usr/local/bin/

# Windows (x86_64)
# Download the ZIP file from the releases page and extract kac.exe
```

#### Build from Source
```bash
# Using go install
go install github.com/janfonas/kafka-admin-cli@latest

# Or clone and build
git clone https://github.com/janfonas/kafka-admin-cli.git
cd kafka-admin-cli
./build.sh
```

### Basic Usage

```bash
# List topics (kubectl-style)
kac get topics

# Create a topic (kubectl-style)
kac create topic mytopic --partitions 3 --replication-factor 1

# Manage consumer groups (kubectl-style)
kac get consumergroups
```

> **Note**: The CLI supports both kubectl-style (verb-first) and legacy (resource-first) command formats. The kubectl-style is recommended for new users.

## Features

### Topic Management
- Create topics with custom partitions and replication factors
- Modify topics
- Delete topics
- List all topics
- View detailed topic configuration

### ACL Management (experimental)
- Create and delete ACLs
- Modify ACLs
- List all ACLs
- View detailed ACL information
- Support for various resource types and operations

### Consumer Group Management
- List all consumer groups
- View detailed consumer group information
  - Member assignments
  - Partition offsets
  - Consumer lag
- Modify consumer group offsets

## Authentication and Security

### Supported Authentication Methods
- SASL/SCRAM-SHA-512 (default)
- SASL/PLAIN
- TLS with custom CA certificates
- TLS with self-signed certificates

### Security Options
```bash
# Using password prompt (recommended)
kac --brokers kafka1:9092 --username alice --prompt-password get topics

# Using password from stdin
echo "mysecret" | kac --brokers kafka1:9092 --username alice --prompt-password get topics

# Using password flag (not recommended)
kac --brokers kafka1:9092 --username alice --password secret get topics

# Using custom CA certificate
kac --brokers kafka1:9092 --username alice --prompt-password \
    --ca-cert /path/to/ca.crt get topics

# Using self-signed certificates
kac --brokers kafka1:9092 --username alice --prompt-password --insecure get topics
```

## Command Reference

### Commands

#### Version
```bash
# Display version information
kac version
```
Shows detailed version information including:
- Version number
- Git commit hash
- Build date
- Go version
- OS/Architecture

### Global Flags
- `--brokers, -b`: Kafka broker list (comma-separated)
- `--username, -u`: SASL username
- `--password, -w`: SASL password (use -P to prompt for password)
- `--prompt-password, -P`: Prompt for password or read from stdin
- `--ca-cert`: CA certificate file path
- `--sasl-mechanism`: Authentication mechanism (SCRAM-SHA-512 or PLAIN)
- `--insecure`: Skip TLS certificate verification

### Topic Commands

#### Kubectl-style (Recommended)
```bash
# Create topic
kac create topic mytopic --partitions 6 --replication-factor 3

# List all topics
kac get topics

# Get specific topic details
kac get topic mytopic

# Delete topic
kac delete topic mytopic

# Modify topic configuration
kac modify topic mytopic --config retention.ms=86400000
```

#### Legacy style
```bash
# Create topic
kac topic create mytopic --partitions 6 --replication-factor 3

# List topics
kac topic list

# Get topic details
kac topic get mytopic

# Delete topic
kac topic delete mytopic
```

### ACL Commands

#### Kubectl-style (Recommended)
```bash
# Create ACL
kac create acl \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice \
  --host "*" \
  --operation READ \
  --permission ALLOW

# List all ACLs
kac get acls

# Get specific ACL details
kac get acl \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice

# Delete ACL
kac delete acl \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice \
  --operation READ \
  --permission ALLOW

# Modify ACL
kac modify acl \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice \
  --operation READ \
  --permission ALLOW \
  --new-permission DENY
```

#### Legacy style
```bash
# Create ACL
kac acl create \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice \
  --host "*" \
  --operation READ \
  --permission ALLOW

# List ACLs
kac acl list

# Get ACL details
kac acl get \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice

# Delete ACL
kac acl delete \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice \
  --operation READ \
  --permission ALLOW
```

### Consumer Group Commands

#### Kubectl-style (Recommended)
```bash
# List all consumer groups
kac get consumergroups

# Get specific group details
kac get consumergroup my-group-id

# Set consumer group offsets
kac set-offsets consumergroup my-group-id my-topic 0 1000

# Delete consumer group
kac delete consumergroup my-group-id
```

#### Legacy style
```bash
# List consumer groups
kac consumergroup list

# Get group details
kac consumergroup get my-group-id

# Set group offsets
kac consumergroup set-offsets my-group-id my-topic 0 1000
```

## Build Information

The build script (`build.sh`) provides:
- Version information from git tags
- CGO disabled for better portability
- Stripped debug information for smaller binary size
- Dependency management with `go mod tidy`

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Credits

Copyright 2024 Jan Harald Fonås

Created by Jan Harald Fonås with the assistance of an LLM.

### Built Using
- [franz-go](https://github.com/twmb/franz-go) - A feature-complete, pure Go Kafka client (Apache-2.0)
- [cobra](https://github.com/spf13/cobra) - A library for creating powerful modern CLI applications (Apache-2.0)
- [kcat](https://github.com/edenhill/kcat) - Inspiration for the CLI design and functionality
