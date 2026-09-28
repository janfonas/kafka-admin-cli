package cmd

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Supported output formats for ACL commands.
const (
	outputTable   = "table"
	outputStrimzi = "strimzi"
	outputJSON    = "json"
)

var validOutputFormats = []string{outputTable, outputStrimzi}

var aclOutputFormats = []string{outputTable, outputStrimzi, outputJSON}

func validateACLOutputFormat(format string) error {
	if !slices.Contains(aclOutputFormats, format) {
		return fmt.Errorf("invalid --output %q: use %s", format, strings.Join(aclOutputFormats, ", "))
	}
	return nil
}

type strimziACLResource struct {
	Type        string  `yaml:"type"`
	Name        *string `yaml:"name,omitempty"`
	PatternType string  `yaml:"patternType,omitempty"`
}

type strimziACL struct {
	Resource   strimziACLResource `yaml:"resource"`
	Operations []string           `yaml:"operations"`
	Host       *string            `yaml:"host,omitempty"`
	Type       string             `yaml:"type,omitempty"`
}

type strimziAuthorization struct {
	Type string       `yaml:"type"`
	ACLs []strimziACL `yaml:"acls"`
}

type strimziUserSpec struct {
	Authentication *strimziUserAuthentication `yaml:"authentication,omitempty"`
	Authorization  strimziAuthorization       `yaml:"authorization"`
}

type strimziUserAuthentication struct {
	Type string `yaml:"type"`
}

func strimziUserIdentity(principal, tlsAuthentication string) (string, *strimziUserAuthentication, error) {
	name := strings.TrimPrefix(principal, "User:")
	var authentication *strimziUserAuthentication
	if strings.HasPrefix(name, "CN=") {
		name = strings.TrimPrefix(name, "CN=")
		if tlsAuthentication == "" {
			tlsAuthentication = tlsAuthenticationExternal
		}
		// Both TLS modes reconstruct CN=<name>; only tls asks the operator to issue credentials.
		authentication = &strimziUserAuthentication{Type: tlsAuthentication}
	}
	if !isValidKubernetesName(name) {
		return "", nil, fmt.Errorf("cannot export principal %q as a KafkaUser: resource name %q must be a Kubernetes DNS subdomain (at most 253 lowercase letters, digits, '-' or '.', with alphanumeric label boundaries); TLS principals must be CN=<name> without additional DN attributes or escapes", principal, name)
	}
	if authentication != nil && authentication.Type == tlsAuthenticationManaged && len(name) > 64 {
		return "", nil, fmt.Errorf("cannot export principal %q with --tls-authentication tls: Strimzi-managed certificate common names must be at most 64 characters", principal)
	}
	return name, authentication, nil
}

// formatACLTable prints ACL resources in the default human-readable table format.
func formatACLTable(w io.Writer, resources []kmsg.DescribeACLsResponseResource) {
	for _, resource := range resources {
		fmt.Fprintf(w, "Resource Type: %v\n", resource.ResourceType)
		fmt.Fprintf(w, "Resource Name: %s\n", resource.ResourceName)
		fmt.Fprintln(w, "ACLs:")
		for _, acl := range resource.ACLs {
			fmt.Fprintf(w, "  Principal: %s\n", acl.Principal)
			fmt.Fprintf(w, "  Host: %s\n", acl.Host)
			fmt.Fprintf(w, "  Operation: %v\n", acl.Operation)
			fmt.Fprintf(w, "  Permission Type: %v\n", acl.PermissionType)
			fmt.Fprintln(w)
		}
	}
}

// formatACLStrimzi renders ACL resources as a Strimzi KafkaUser CR YAML manifest.
// The output groups ACLs by principal, producing one KafkaUser document per principal.
func formatACLStrimzi(w io.Writer, resources []kmsg.DescribeACLsResponseResource, options aclExportOptions) error {
	if err := options.validate(); err != nil {
		return err
	}
	// Group ACLs by principal
	type aclEntry struct {
		resource kmsg.DescribeACLsResponseResource
		acl      kmsg.DescribeACLsResponseResourceACL
	}
	byPrincipal := make(map[string][]aclEntry)
	var principalOrder []string

	for _, resource := range resources {
		for _, acl := range resource.ACLs {
			if _, seen := byPrincipal[acl.Principal]; !seen {
				principalOrder = append(principalOrder, acl.Principal)
			}
			byPrincipal[acl.Principal] = append(byPrincipal[acl.Principal], aclEntry{resource: resource, acl: acl})
		}
	}

	var manifests []strimziManifest[strimziUserSpec]
	principalByName := make(map[string]string)
	for _, principal := range principalOrder {
		userName, authentication, err := strimziUserIdentity(principal, options.tlsAuthentication)
		if err != nil {
			return err
		}
		if previous, exists := principalByName[userName]; exists {
			return fmt.Errorf("cannot export principals %q and %q: both map to KafkaUser %q; refusing to overwrite either identity", previous, principal, userName)
		}
		principalByName[userName] = principal
		if authentication == nil && options.discoverSCRAM {
			authentication, err = discoveredSCRAMAuthentication(userName, options.scramCredentials)
			if err != nil {
				return err
			}
		}
		manifest := strimziManifest[strimziUserSpec]{
			APIVersion: strimziAPIVersionOrDefault(options.apiVersion),
			Kind:       "KafkaUser",
			Metadata:   strimziMetadata{Name: userName},
			Spec: strimziUserSpec{
				Authentication: authentication,
				Authorization:  strimziAuthorization{Type: "simple"},
			},
		}

		// Group by resource + host + permission to merge operations
		type aclKey struct {
			resourceType   kmsg.ACLResourceType
			resourceName   string
			patternType    kmsg.ACLResourcePatternType
			host           string
			permissionType kmsg.ACLPermissionType
		}
		type mergedACL struct {
			key        aclKey
			operations []kmsg.ACLOperation
		}

		var merged []mergedACL
		keyIndex := make(map[aclKey]int)

		for _, entry := range byPrincipal[principal] {
			k := aclKey{
				resourceType:   entry.resource.ResourceType,
				resourceName:   entry.resource.ResourceName,
				patternType:    entry.resource.ResourcePatternType,
				host:           entry.acl.Host,
				permissionType: entry.acl.PermissionType,
			}
			if idx, ok := keyIndex[k]; ok {
				merged[idx].operations = append(merged[idx].operations, entry.acl.Operation)
			} else {
				keyIndex[k] = len(merged)
				merged = append(merged, mergedACL{
					key:        k,
					operations: []kmsg.ACLOperation{entry.acl.Operation},
				})
			}
		}

		for _, m := range merged {
			resourceType, err := strimziResourceType(m.key.resourceType)
			if err != nil {
				return fmt.Errorf("cannot export ACLs for principal %q: %w", principal, err)
			}
			acl := strimziACL{
				Resource: strimziACLResource{
					Type: resourceType,
				},
			}
			if m.key.resourceType != kmsg.ACLResourceTypeCluster {
				name := m.key.resourceName
				acl.Resource.Name = &name
				acl.Resource.PatternType, err = strimziPatternType(m.key.patternType)
				if err != nil {
					return fmt.Errorf("cannot export ACLs for principal %q: %w", principal, err)
				}
			}
			for _, op := range m.operations {
				operation, err := strimziOperation(op)
				if err != nil {
					return fmt.Errorf("cannot export ACLs for principal %q: %w", principal, err)
				}
				acl.Operations = append(acl.Operations, operation)
			}
			if m.key.host != "*" {
				host := m.key.host
				acl.Host = &host
			}
			switch m.key.permissionType {
			case kmsg.ACLPermissionTypeAllow:
			case kmsg.ACLPermissionTypeDeny:
				acl.Type = "deny"
			default:
				return fmt.Errorf("cannot export ACLs for principal %q: permission %v is not supported by Strimzi KafkaUser ACL rules", principal, m.key.permissionType)
			}
			manifest.Spec.Authorization.ACLs = append(manifest.Spec.Authorization.ACLs, acl)
		}
		manifests = append(manifests, manifest)
	}
	return writeStrimziManifests(w, manifests)
}

// strimziResourceType maps Kafka ACLResourceType to the Strimzi resource types KafkaUser supports.
func strimziResourceType(t kmsg.ACLResourceType) (string, error) {
	switch t {
	case kmsg.ACLResourceTypeTopic:
		return "topic", nil
	case kmsg.ACLResourceTypeGroup:
		return "group", nil
	case kmsg.ACLResourceTypeCluster:
		return "cluster", nil
	case kmsg.ACLResourceTypeTransactionalId:
		return "transactionalId", nil
	default:
		return "", fmt.Errorf("resource type %v is not supported by Strimzi KafkaUser ACL rules", t)
	}
}

// strimziPatternType maps Kafka ACLResourcePatternType to Strimzi patternType string.
func strimziPatternType(t kmsg.ACLResourcePatternType) (string, error) {
	switch t {
	case kmsg.ACLResourcePatternTypeLiteral:
		return "literal", nil
	case kmsg.ACLResourcePatternTypePrefixed:
		return "prefix", nil
	default:
		return "", fmt.Errorf("pattern type %v is not supported by Strimzi KafkaUser ACL rules", t)
	}
}

// strimziOperation maps Kafka ACLOperation to Strimzi operation string.
func strimziOperation(op kmsg.ACLOperation) (string, error) {
	switch op {
	case kmsg.ACLOperationAll:
		return "All", nil
	case kmsg.ACLOperationRead:
		return "Read", nil
	case kmsg.ACLOperationWrite:
		return "Write", nil
	case kmsg.ACLOperationCreate:
		return "Create", nil
	case kmsg.ACLOperationDelete:
		return "Delete", nil
	case kmsg.ACLOperationAlter:
		return "Alter", nil
	case kmsg.ACLOperationDescribe:
		return "Describe", nil
	case kmsg.ACLOperationClusterAction:
		return "ClusterAction", nil
	case kmsg.ACLOperationDescribeConfigs:
		return "DescribeConfigs", nil
	case kmsg.ACLOperationAlterConfigs:
		return "AlterConfigs", nil
	case kmsg.ACLOperationIdempotentWrite:
		return "IdempotentWrite", nil
	default:
		return "", fmt.Errorf("operation %v is not supported by Strimzi KafkaUser ACL rules", op)
	}
}
