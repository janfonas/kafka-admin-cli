package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Supported output formats for ACL commands.
const (
	outputTable   = "table"
	outputStrimzi = "strimzi"
)

var validOutputFormats = []string{outputTable, outputStrimzi}

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
			APIVersion: "kafka.strimzi.io/v1beta2",
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
			acl := strimziACL{
				Resource: strimziACLResource{
					Type: strimziResourceType(m.key.resourceType),
				},
			}
			if m.key.resourceType != kmsg.ACLResourceTypeCluster {
				name := m.key.resourceName
				acl.Resource.Name = &name
				acl.Resource.PatternType = strimziPatternType(m.key.patternType)
			}
			for _, op := range m.operations {
				acl.Operations = append(acl.Operations, strimziOperation(op))
			}
			if m.key.host != "*" {
				host := m.key.host
				acl.Host = &host
			}
			if m.key.permissionType != kmsg.ACLPermissionTypeAllow {
				acl.Type = strimziPermission(m.key.permissionType)
			}
			manifest.Spec.Authorization.ACLs = append(manifest.Spec.Authorization.ACLs, acl)
		}
		manifests = append(manifests, manifest)
	}
	return writeStrimziManifests(w, manifests)
}

// strimziResourceType maps Kafka ACLResourceType to Strimzi resource type string.
func strimziResourceType(t kmsg.ACLResourceType) string {
	switch t {
	case kmsg.ACLResourceTypeTopic:
		return "topic"
	case kmsg.ACLResourceTypeGroup:
		return "group"
	case kmsg.ACLResourceTypeCluster:
		return "cluster"
	case kmsg.ACLResourceTypeTransactionalId:
		return "transactionalId"
	case kmsg.ACLResourceTypeDelegationToken:
		return "delegationToken"
	default:
		return strings.ToLower(t.String())
	}
}

// strimziPatternType maps Kafka ACLResourcePatternType to Strimzi patternType string.
func strimziPatternType(t kmsg.ACLResourcePatternType) string {
	switch t {
	case kmsg.ACLResourcePatternTypeLiteral:
		return "literal"
	case kmsg.ACLResourcePatternTypePrefixed:
		return "prefix"
	default:
		return "literal"
	}
}

// strimziOperation maps Kafka ACLOperation to Strimzi operation string.
func strimziOperation(op kmsg.ACLOperation) string {
	switch op {
	case kmsg.ACLOperationAll:
		return "All"
	case kmsg.ACLOperationRead:
		return "Read"
	case kmsg.ACLOperationWrite:
		return "Write"
	case kmsg.ACLOperationCreate:
		return "Create"
	case kmsg.ACLOperationDelete:
		return "Delete"
	case kmsg.ACLOperationAlter:
		return "Alter"
	case kmsg.ACLOperationDescribe:
		return "Describe"
	case kmsg.ACLOperationClusterAction:
		return "ClusterAction"
	case kmsg.ACLOperationDescribeConfigs:
		return "DescribeConfigs"
	case kmsg.ACLOperationAlterConfigs:
		return "AlterConfigs"
	case kmsg.ACLOperationIdempotentWrite:
		return "IdempotentWrite"
	default:
		return op.String()
	}
}

// strimziPermission maps Kafka ACLPermissionType to Strimzi acl type string.
func strimziPermission(p kmsg.ACLPermissionType) string {
	switch p {
	case kmsg.ACLPermissionTypeAllow:
		return "allow"
	case kmsg.ACLPermissionTypeDeny:
		return "deny"
	default:
		return strings.ToLower(p.String())
	}
}
