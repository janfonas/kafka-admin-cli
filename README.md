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
# List topics
kac get topics

# Create a topic
kac create topic mytopic --partitions 3 --replication-factor 1

# Manage consumer groups
kac get consumergroups
```

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

### Credential Storage (Recommended)

Store your credentials securely in your system's keyring for convenient reuse:

```bash
# Login and store credentials (uses system keyring - Keychain/Secret Service/Credential Manager)
kac login --brokers kafka1:9092 --username alice
# Password will be prompted securely

# Now use commands without credentials
kac get topics
kac get consumergroups

# Use named profiles for multiple environments
kac login --profile prod --brokers prod-kafka:9092 --username alice
kac login --profile dev --brokers dev-kafka:9092 --username bob

# Use specific profile
kac --profile prod get topics

# Logout (remove stored credentials)
kac logout
kac logout --profile prod
```

**Security Features:**
- Credentials are encrypted by your OS (macOS Keychain, Linux Secret Service, Windows Credential Manager)
- No plaintext passwords in files or command history
- Supports multiple profiles for different environments

### Security Options (Alternative Methods)
```bash
# Using password prompt (good for one-time commands)
kac --brokers kafka1:9092 --username alice --prompt-password get topics

# Using password from stdin (good for automation)
echo "mysecret" | kac --brokers kafka1:9092 --username alice --prompt-password get topics

# Using password flag (not recommended - visible in process list)
kac --brokers kafka1:9092 --username alice --password secret get topics

# Using custom CA certificate
kac --brokers kafka1:9092 --username alice --prompt-password \
    --ca-cert /path/to/ca.crt get topics

# Using self-signed certificates
kac --brokers kafka1:9092 --username alice --prompt-password --insecure get topics
```

### Credential Priority Order
When using stored credentials, the following priority order applies:
1. Command-line flags (`--brokers`, `--username`, `--password`)
2. Environment variables (`KAC_BROKERS`, `KAC_USERNAME`, `KAC_PASSWORD`)
3. Stored profile (from `kac login`)
4. Interactive prompt (if `--prompt-password` is used)

## Command Reference

### Commands

#### Login / Logout

```bash
# Login and store credentials
kac login --brokers kafka1:9092 --username alice
kac login --profile prod --brokers kafka1:9092 --username alice --sasl-mechanism PLAIN

# Logout and remove credentials
kac logout
kac logout --profile prod
```

**Login Options:**
- `--profile`: Profile name to store credentials under (default: "default")
- All global connection flags can be used and will be stored

**Logout Options:**
- `--profile`: Profile name to remove (default: "default")

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
- `--profile`: Profile name to use for stored credentials (default: "default")
- `--brokers, -b`: Kafka broker list (comma-separated)
- `--username, -u`: SASL username
- `--password, -w`: SASL password (use -P to prompt for password)
- `--prompt-password, -P`: Prompt for password or read from stdin
- `--ca-cert`: CA certificate file path
- `--sasl-mechanism`: Authentication mechanism (SCRAM-SHA-512 or PLAIN)
- `--insecure`: Skip TLS certificate verification

### Topic Commands

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

### ACL Commands

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

### Consumer Group Commands

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
