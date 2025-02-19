# Kafka Admin Client (kac)

A command-line interface for managing Apache Kafka topics and ACLs using the Franz-go Kafka client.

Written by Jan Harald Fonås with the help of an LLM.

## Features

- Topic Management
  - Create topics
  - Delete topics
  - List topics
- ACL Management
  - Create ACLs
  - Delete ACLs
  - List ACLs
- Consumer Group Management
  - List consumer groups
  - Get consumer group details (members, assignments, offsets)
  - Set consumer group offsets

## Installation

### Option 1: Using go install

```bash
go install github.com/janfonas/kafka-admin-cli@latest
```

### Option 2: Building from source

1. Clone the repository:
```bash
git clone https://github.com/janfonas/kafka-admin-cli.git
cd kafka-admin-cli
```

2. Build the binary:
```bash
./build.sh
```

This will create a `kac` binary in the current directory. The build script:
- Includes version information from git tags
- Disables CGO for better portability
- Strips debug information for smaller binary size
- Runs `go mod tidy` to ensure dependencies are up to date

## Usage

### Global Flags

- `--brokers, -b`: Kafka broker list (comma-separated) (default: "localhost:9092")
- `--username, -u`: SASL username
- `--password, -w`: SASL password
- `--ca-cert`: Path to CA certificate file for TLS connections
- `--sasl-mechanism`: SASL mechanism (SCRAM-SHA-512 or PLAIN) (default: SCRAM-SHA-512)
- `--insecure`: Skip TLS certificate verification

### Topic Management

Create a topic:
```bash
kac topic create mytopic --partitions 3 --replication-factor 1
```

Delete a topic:
```bash
kac topic delete mytopic
```

List topics:
```bash
kac topic list
```

Get topic details:
```bash
kac topic get mytopic
```

### ACL Management

Create an ACL:
```bash
kac acl create \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice \
  --host "*" \
  --operation READ \
  --permission ALLOW
```

Delete an ACL:
```bash
kac acl delete \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice \
  --host "*" \
  --operation READ \
  --permission ALLOW
```

List ACLs:
```bash
kac acl list
```

Get ACL details:
```bash
kac acl get \
  --resource-type TOPIC \
  --resource-name mytopic \
  --principal User:alice
```

## Authentication

The CLI supports SCRAM-SHA-512 authentication and custom CA certificates for TLS connections. Provide your credentials and CA certificate using the global flags:

```bash
# Using SCRAM-SHA-512 authentication (default)
kac --brokers kafka1:9092,kafka2:9092 --username alice --password secret topic list

# Using SASL/PLAIN authentication
kac --brokers kafka1:9092,kafka2:9092 --username alice --password secret --sasl-mechanism PLAIN topic list

# Using authentication with custom CA certificate
kac --brokers kafka1:9092,kafka2:9092 --username alice --password secret --sasl-mechanism PLAIN --ca-cert /path/to/ca.crt topic list

# Using authentication with self-signed certificates
kac --brokers kafka1:9092,kafka2:9092 --username alice --password secret --insecure topic list
```

## Examples

1. Create a topic with custom partitions and replication factor:
```bash
kac topic create orders --partitions 6 --replication-factor 3
```

2. Grant read access to a consumer group:
```bash
kac acl create \
  --resource-type GROUP \
  --resource-name mygroup \
  --principal User:bob \
  --host "*" \
  --operation READ \
  --permission ALLOW
```

3. List all topics with authentication:
```bash
kac --brokers kafka1:9092,kafka2:9092 --username alice --password secret topic list
```

### Consumer Group Management

List consumer groups:
```bash
kac consumergroup list
```

Get consumer group details:
```bash
kac consumergroup get my-group-id
```

Set consumer group offsets:
```bash
kac consumergroup set-offsets my-group-id my-topic --partition 0 --offset 1000
```

Example output for consumer group details:
```
Group ID: my-group-id
State: Stable

Members:
  Client ID: consumer-1
  Client Host: consumer-1.example.com
  Assignments:
    Topic: my-topic
    Partitions: [0, 1, 2]

Offsets:
  Topic: my-topic
    Partition 0: Current=1000, End=1500, Lag=500
    Partition 1: Current=2000, End=2500, Lag=500
    Partition 2: Current=3000, End=3500, Lag=500
