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

```bash
# Using go install
go install github.com/janfonas/kafka-admin-cli@latest

# Or build from source
git clone https://github.com/janfonas/kafka-admin-cli.git
cd kafka-admin-cli
./build.sh
```

### Basic Usage

```bash
# List topics
kac topic list

# Create a topic
kac topic create mytopic --partitions 3 --replication-factor 1

# Manage consumer groups
kac consumergroup list
```

## Features

### Topic Management
- Create topics with custom partitions and replication factors
- Delete topics
- List all topics
- View detailed topic configuration

### ACL Management
- Create and delete ACLs
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
# SASL Authentication
kac --brokers kafka1:9092 --username alice --password secret topic list

# Custom CA Certificate
kac --brokers kafka1:9092 --username alice --password secret \
    --ca-cert /path/to/ca.crt topic list

# Self-signed Certificates
kac --brokers kafka1:9092 --username alice --password secret --insecure topic list
```

## Command Reference

### Global Flags
- `--brokers, -b`: Kafka broker list (comma-separated)
- `--username, -u`: SASL username
- `--password, -w`: SASL password
- `--ca-cert`: CA certificate file path
- `--sasl-mechanism`: Authentication mechanism (SCRAM-SHA-512 or PLAIN)
- `--insecure`: Skip TLS certificate verification

### Topic Commands
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
```bash
# List consumer groups
kac consumergroup list

# Get group details
kac consumergroup get my-group-id

# Set group offsets
kac consumergroup set-offsets my-group-id my-topic \
  --partition 0 --offset 1000
```

## Build Information

The build script (`build.sh`) provides:
- Version information from git tags
- CGO disabled for better portability
- Stripped debug information for smaller binary size
- Dependency management with `go mod tidy`

## Credits

Created by Jan Harald Fonås with the assistance of an LLM.
