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

> **Note:** Releases up to and including the current one ship the binary as
> `kafka-admin-cli` — for those, rename it during install:
> `sudo mv kafka-admin-cli /usr/local/bin/kac`.

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
- Modify topic configuration
- Delete topics
- List all topics
- View detailed topic configuration
- Export topics as Strimzi `KafkaTopic` CRD YAML (`-o strimzi`)

### ACL Management
- Create and delete ACLs
- Modify ACLs
- List all ACLs
- View detailed ACL information with optional filters
- Support for various resource types and operations
- Export ACLs as Strimzi `KafkaUser` CRD YAML (`-o strimzi`)
- Export ACLs as a versioned, lossless JSON document (`-o json`) for review,
  diffing, and offline analysis

### Consumer Group Management
- List all consumer groups
- View detailed consumer group information
  - Member assignments
  - Partition offsets
  - Consumer lag
- Modify consumer group offsets

### Output Formats
- **table** (default) — human-readable tabular output
- **strimzi** — Strimzi CRD YAML manifests, ready to apply with `kubectl`
- **json** (ACL commands only) — a versioned `KafkaACLExport` document with every
  ACL binding exactly as the broker reports it

When using a structured output format (e.g. `strimzi` or `json`), connection status messages
are suppressed so output can be safely piped to tools like `yq`, `jq`, or `kubectl apply`.
ACL commands reject unknown `--output` values instead of falling back to the table.

### Error Handling
Every non-zero Kafka error code is reported as a failure, including retriable
codes such as `REQUEST_TIMED_OUT`. Messages name the operation, the numeric code,
Kafka's error name, and any broker-provided detail, for example:

```text
failed to list ACLs (error code 7): REQUEST_TIMED_OUT: The request timed out.
```

A timed-out or rejected request therefore never looks like an empty result.

### Shell Completion
Dynamic shell completion for bash, zsh, fish, and PowerShell. Tab-complete topic
names, consumer group IDs, ACL resource types, principal names, output formats,
and profile names — all fetched live from your Kafka cluster.

```bash
# Bash
source <(kac completion bash)

# Zsh (add to ~/.zshrc)
source <(kac completion zsh)

# Fish
kac completion fish | source

# PowerShell
kac completion powershell | Out-String | Invoke-Expression
```

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

# List all stored profiles
kac profile list

# Switch active profile (will be used by default)
kac profile switch prod

# Now commands automatically use the 'prod' profile
kac get topics

# Override with a specific profile for a single command
kac --profile dev get topics

# Logout (remove stored credentials)
kac logout
kac logout --profile prod
```

**Security Features:**
- Credentials are encrypted by your OS (macOS Keychain, Linux Secret Service, Windows Credential Manager)
- No plaintext passwords in files or command history
- Supports multiple profiles for different environments
- Active profile is automatically used when `--profile` flag is not specified

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
2. Active profile (set via `kac profile switch`)
3. Default profile (if `--profile` flag is not specified)
4. Interactive prompt (if `--prompt-password` is used)

## Command Reference

### Commands

#### Login / Logout / Profile Management

```bash
# Login and store credentials
kac login --brokers kafka1:9092 --username alice
kac login --profile prod --brokers kafka1:9092 --username alice --sasl-mechanism PLAIN

# List all stored profiles
kac profile list

# Switch to a different profile (becomes the default)
kac profile switch prod

# Logout and remove credentials
kac logout
kac logout --profile prod
```

**Login Options:**
- `--profile`: Profile name to store credentials under (default: "default")
- All global connection flags can be used and will be stored

**Profile List:**
- Shows all stored profiles with their connection details
- Indicates which profile is currently active with an asterisk (*)

**Profile Switch:**
- Sets the active profile that will be used by default for all commands
- The active profile is stored in `~/.kac/active_profile`

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

# Export a single topic as Strimzi KafkaTopic YAML
kac get topic mytopic -o strimzi

# Export all topics as Strimzi KafkaTopic YAML (multi-document)
kac get topics -o strimzi

# Pipe to kubectl
kac get topic mytopic -o strimzi | kubectl apply -f -

# Target a Strimzi release that serves only the v1 API
kac get topics -o strimzi --strimzi-api-version kafka.strimzi.io/v1
```

`--strimzi-api-version` selects the `apiVersion` of exported `KafkaTopic` and
`KafkaUser` manifests. The default remains `kafka.strimzi.io/v1beta2` for
existing clusters; use `kafka.strimzi.io/v1` for Strimzi releases that no longer
serve `v1beta2` (Strimzi 1.2, for example, serves only `v1`). Check which versions
your cluster serves with `kubectl api-resources --api-group=kafka.strimzi.io`.
The flag requires `-o strimzi`.

Topic exports retain valid Kubernetes names. Other names are mapped using
Strimzi's legacy Topic Operator convention: a sanitized prefix followed by `---`
and a hash of the original name. The original Kafka name is preserved in
`spec.topicName`, so importing the manifest does not rename the topic. A trailing
dot exposed by prefix truncation is removed to keep the resource name valid.
Multiple topics are emitted as YAML documents separated by `---`; name collisions
cause the export to fail before writing any manifests.

A topic whose metadata or configuration cannot be read (for example because the
user lacks `DescribeConfigs` on it) is skipped with a warning on stderr instead of
being exported without its configuration. Check stderr before applying a
multi-topic export: a skipped topic is missing from the output.

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

# List all ACL principals
kac get acls

# Get ACL details (all filters are optional)
kac get acl --principal User:alice
kac get acl --resource-type TOPIC --resource-name mytopic
kac get acl --resource-type TOPIC --resource-name mytopic --principal User:alice

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

# Export ACLs as Strimzi KafkaUser YAML (one document per principal)
kac get acl --principal User:alice -o strimzi
kac get acls -o strimzi

# Export ACLs for a TLS principal, preserving the CN-based Kafka identity
kac get acl --principal 'User:CN=nt-ibm-es-kafka-user' -o strimzi

# Export existing SCRAM-SHA-512 authentication metadata
kac get acl --principal User:superuser -o strimzi --discover-scram

# Export a mixed user list with Strimzi-managed TLS and discovered SCRAM users
kac get acls -o strimzi --discover-scram --tls-authentication tls

# Pipe to kubectl
kac get acl --principal User:alice -o strimzi | kubectl apply -f -

# Export every ACL binding as versioned JSON
kac get acls -o json > acls.json

# Export a filtered subset (recorded in the document as a partial listing)
kac get acl --principal User:alice -o json
```

**Output format flag (`-o, --output`):**
- `table` (default): human-readable text
- `json`: a lossless `KafkaACLExport` document; see [JSON ACL Export](#json-acl-export).
- `strimzi`: Strimzi `KafkaUser` CRD YAML with `spec.authorization.acls`.
  Operations sharing the same resource, host, and permission are merged.
  The default `type: allow` is omitted since it is the Strimzi default.
  Cluster ACL resources contain only `type: cluster`; Kafka's cluster resource
  name and pattern type are not fields in the Strimzi cluster ACL schema.
  Values a `KafkaUser` cannot represent (delegation-token or user resources,
  token operations, or non-literal/prefix patterns) fail the export instead of
  producing a manifest Strimzi would reject. `--strimzi-api-version` applies here
  as for topics.

ACL exports produce one document per principal, separated by `---`.
`User:alice` becomes a KafkaUser named `alice` with authentication omitted.
`User:CN=nt-ibm-es-kafka-user` becomes a KafkaUser named `nt-ibm-es-kafka-user`
with `spec.authentication.type: tls-external`. This tells Strimzi to apply ACLs
to `CN=nt-ibm-es-kafka-user` without generating a new certificate.

**Authentication export options (require `-o strimzi`):**
- `--tls-authentication tls-external|tls`: selects the authentication type for
  CN-based principals only. The default is `tls-external`; use `tls` when the
  Kubernetes operator should manage their certificates, including exports
  intended for existing operator-managed TLS users. Managed TLS names must
  be at most 64 characters, matching Strimzi's certificate CN limit.
- `--discover-scram`: queries Kafka for SCRAM credential metadata for the
  exported non-TLS usernames. Users with a registered SCRAM-SHA-512 credential
  receive `authentication.type: scram-sha-512`. Users without SCRAM credentials
  keep authentication omitted. SCRAM-SHA-256-only or otherwise unsupported
  mechanisms fail the export, since Strimzi cannot represent them. This requires
  Kafka 2.7+ and `Describe` permission on the cluster. Unsupported APIs,
  authorization failures, and incomplete responses are errors, not silent
  fallbacks. Discovery is disabled by default and never queries unrelated users.

These options describe exported users, not the CLI's connection authentication.
Kafka ACLs cannot distinguish operator-managed from external certificates.
SCRAM discovery identifies registered mechanisms, but cannot recover passwords,
Secret references, or the operator's credential-management configuration.
Review authentication settings before applying an export: changing them can
cause the operator to generate or manage credentials. This is an ACL export,
not a complete backup of existing KafkaUser resources.

The resulting KafkaUser names must be valid Kubernetes DNS subdomain names:
at most 253 lowercase letters, digits, hyphens, or dots, with each dot-separated
label starting and ending with a letter or digit. Arbitrary distinguished names
with additional attributes or escapes are not reduced to their CN, because that
would change the Kafka identity. Invalid names and collisions such as
`User:alice` and `User:CN=alice` cause the export to fail before emitting any
manifests; user identities are never silently renamed or merged.

Broker-provided strings in both ACL and topic exports are serialized as YAML
data, not interpreted as manifest structure.

#### JSON ACL Export

`-o json` writes one JSON document containing every binding exactly as the
broker reports it: no operations are merged, no defaults are dropped, and values
use Kafka's own names (`TOPIC`, `PREFIXED`, `READ`, `ALLOW`, ...).

```json
{
  "kind": "KafkaACLExport",
  "schemaVersion": 1,
  "clusterId": "Hm2pQ4rXS7yN0vB8kLd3aw",
  "capturedAt": "2026-09-28T10:00:00Z",
  "filter": {},
  "bindings": [
    {
      "principal": "User:CN=orders-app",
      "host": "*",
      "resourceType": "TOPIC",
      "resourceName": "orders",
      "patternType": "LITERAL",
      "operation": "READ",
      "permission": "ALLOW"
    }
  ]
}
```

- `schemaVersion` is incremented on any incompatible change; consumers should
  reject versions they do not know.
- `clusterId` is the Kafka cluster ID from broker metadata, and `capturedAt`
  is the UTC time the listing was requested.
- `filter` records the filters used. `{}` means a complete listing; any
  populated field (`resourceType`, `resourceName`, `principal`) means the
  document is a partial listing and must not be treated as a full inventory.
- `bindings` is sorted deterministically, so two exports of the same cluster
  can be compared with `diff`. An empty listing is written as `[]` and exits
  successfully; `kac get acl` in table mode still reports "no ACLs found".
- The export fails without writing anything if the broker returns an error,
  a non-concrete value (`ANY`, `MATCH`, `UNKNOWN`), or duplicate bindings.

The JSON contains principals and resource names but no credentials. Treat it as
sensitive operational data.

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

Copyright 2024-2026 Jan Harald Fonås

Created by [Jan Harald Fonås](https://github.com/janfonas) with the assistance of an LLM.

### Contributors

A heartfelt thank you to everyone who has contributed to `kac`:

- [Jan Harald Fonås (@janfonas)](https://github.com/janfonas) — Original author and maintainer.
- [Robert Goldsmith (@far-blue)](https://github.com/far-blue) — Fixed a subtle but important
  bug in `GetConsumerGroup` where partition offsets and lag values could be mis-attributed
  to the wrong partition because the code zipped request and response partitions by array
  index. Kafka makes no ordering guarantee on response partitions, so the fix matches them
  by partition ID instead ([PR #1](https://github.com/janfonas/kafka-admin-cli/pull/1)).

### Built Using
- [franz-go](https://github.com/twmb/franz-go) - A feature-complete, pure Go Kafka client (Apache-2.0)
- [cobra](https://github.com/spf13/cobra) - A library for creating powerful modern CLI applications (Apache-2.0)
- [kcat](https://github.com/edenhill/kcat) - Inspiration for the CLI design and functionality
