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
func formatACLStrimzi(w io.Writer, resources []kmsg.DescribeACLsResponseResource) {
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

	for i, principal := range principalOrder {
		if i > 0 {
			fmt.Fprintln(w, "---")
		}

		userName := principal
		if strings.HasPrefix(principal, "User:") {
			userName = strings.TrimPrefix(principal, "User:")
		}

		fmt.Fprintln(w, "apiVersion: kafka.strimzi.io/v1beta2")
		fmt.Fprintln(w, "kind: KafkaUser")
		fmt.Fprintln(w, "metadata:")
		fmt.Fprintf(w, "  name: %s\n", userName)
		fmt.Fprintln(w, "spec:")
		fmt.Fprintln(w, "  authorization:")
		fmt.Fprintln(w, "    type: simple")
		fmt.Fprintln(w, "    acls:")

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
			fmt.Fprintln(w, "      - resource:")
			fmt.Fprintf(w, "          type: %s\n", strimziResourceType(m.key.resourceType))
			fmt.Fprintf(w, "          name: %s\n", yamlQuoteIfNeeded(m.key.resourceName))
			fmt.Fprintf(w, "          patternType: %s\n", strimziPatternType(m.key.patternType))
			fmt.Fprintln(w, "        operations:")
			for _, op := range m.operations {
				fmt.Fprintf(w, "          - %s\n", strimziOperation(op))
			}
			if m.key.host != "*" {
				fmt.Fprintf(w, "        host: %s\n", yamlQuoteIfNeeded(m.key.host))
			}
			if m.key.permissionType != kmsg.ACLPermissionTypeAllow {
				fmt.Fprintf(w, "        type: %s\n", strimziPermission(m.key.permissionType))
			}
		}
	}
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

// yamlQuoteIfNeeded wraps a value in quotes if it contains special YAML characters.
func yamlQuoteIfNeeded(s string) string {
	if s == "*" || s == "" || strings.ContainsAny(s, ":{}[]&!|>'\"%@`") {
		return fmt.Sprintf("%q", s)
	}
	return s
}
