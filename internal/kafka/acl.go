package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// ACLRequestTimeout represents the default timeout for ACL operations
const ACLRequestTimeout = 10 * time.Second

// CreateAcl Creates a new Access Control List (ACL) entry in Kafka.
// Parameters include resource type (e.g., topic), resource name, principal (user),
// host, operation (e.g., read, write), and permission type (allow/deny).
func (c *Client) CreateAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string) error {
	ctx, cancel := context.WithTimeout(ctx, ACLRequestTimeout)
	defer cancel()

	resourceTypeInt, err := strconv.Atoi(resourceType)
	if err != nil {
		return fmt.Errorf("invalid resource type: %w", err)
	}
	operationInt, err := strconv.Atoi(operation)
	if err != nil {
		return fmt.Errorf("invalid operation: %w", err)
	}
	permissionInt, err := strconv.Atoi(permission)
	if err != nil {
		return fmt.Errorf("invalid permission: %w", err)
	}

	creation := kmsg.NewCreateACLsRequestCreation()
	creation.ResourceType = kmsg.ACLResourceType(resourceTypeInt)
	creation.ResourceName = resourceName
	creation.Principal = principal
	creation.Host = host
	creation.Operation = kmsg.ACLOperation(operationInt)
	creation.PermissionType = kmsg.ACLPermissionType(permissionInt)

	req := kmsg.NewPtrCreateACLsRequest()
	req.Creations = []kmsg.CreateACLsRequestCreation{creation}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to create ACL (timeout=%v): %w", ACLRequestTimeout, err)
	}
	return handleACLCreateError(resp)
}

// DeleteAcl Removes an existing ACL entry from Kafka.
// The parameters must match exactly with an existing ACL entry for it to be deleted.
func (c *Client) DeleteAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string) error {
	ctx, cancel := context.WithTimeout(ctx, ACLRequestTimeout)
	defer cancel()

	resourceTypeInt, err := strconv.Atoi(resourceType)
	if err != nil {
		return fmt.Errorf("invalid resource type: %w", err)
	}
	operationInt, err := strconv.Atoi(operation)
	if err != nil {
		return fmt.Errorf("invalid operation: %w", err)
	}
	permissionInt, err := strconv.Atoi(permission)
	if err != nil {
		return fmt.Errorf("invalid permission: %w", err)
	}

	filter := kmsg.NewDeleteACLsRequestFilter()
	filter.ResourceType = kmsg.ACLResourceType(resourceTypeInt)
	filter.ResourceName = &resourceName
	filter.Principal = &principal
	filter.Host = &host
	filter.Operation = kmsg.ACLOperation(operationInt)
	filter.PermissionType = kmsg.ACLPermissionType(permissionInt)

	req := kmsg.NewPtrDeleteACLsRequest()
	req.Filters = []kmsg.DeleteACLsRequestFilter{filter}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete ACL (timeout=%v): %w", ACLRequestTimeout, err)
	}
	if len(resp.Results) > 0 && resp.Results[0].ErrorCode != 0 {
		return formatACLError("delete ACL", resp.Results[0].ErrorCode)
	}
	return nil
}

// ModifyAcl Updates an existing ACL entry by deleting it and creating a new one
// with the updated permission. This is used to change the permission type (allow/deny)
// while keeping all other ACL parameters the same.
func (c *Client) ModifyAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string, newPermission string) error {
	// First delete the existing ACL
	err := c.DeleteAcl(ctx, resourceType, resourceName, principal, host, operation, permission)
	if err != nil {
		return fmt.Errorf("failed to delete existing ACL: %w", err)
	}

	// Then create the new ACL with updated permission
	err = c.CreateAcl(ctx, resourceType, resourceName, principal, host, operation, newPermission)
	if err != nil {
		return fmt.Errorf("failed to create new ACL: %w", err)
	}

	return nil
}

// GetAcl Retrieves ACL entries matching the specified filters.
// All parameters are optional — empty strings are treated as "any" (match all).
// Returns a list of ACL resources that match the criteria.
func (c *Client) GetAcl(ctx context.Context, resourceType, resourceName, principal string) ([]kmsg.DescribeACLsResponseResource, error) {
	ctx, cancel := context.WithTimeout(ctx, ACLRequestTimeout)
	defer cancel()

	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceTypeAny
	req.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	req.Operation = kmsg.ACLOperationAny
	req.PermissionType = kmsg.ACLPermissionTypeAny

	if resourceType != "" {
		resourceTypeInt, err := strconv.Atoi(resourceType)
		if err != nil {
			return nil, fmt.Errorf("invalid resource type: %w", err)
		}
		req.ResourceType = kmsg.ACLResourceType(resourceTypeInt)
	}
	if resourceName != "" {
		req.ResourceName = &resourceName
	}
	if principal != "" {
		req.Principal = &principal
	}

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get ACL (timeout=%v): %w", ACLRequestTimeout, err)
	}
	if resp.ErrorCode != 0 {
		return nil, formatACLError("get ACL", resp.ErrorCode)
	}
	if len(resp.Resources) == 0 {
		parts := []string{}
		if resourceType != "" {
			parts = append(parts, "resource type "+resourceType)
		}
		if resourceName != "" {
			parts = append(parts, "name "+resourceName)
		}
		if principal != "" {
			parts = append(parts, "principal "+principal)
		}
		filter := "the given filters"
		if len(parts) > 0 {
			filter = strings.Join(parts, ", ")
		}
		return nil, fmt.Errorf("no ACLs found matching %s", filter)
	}
	return resp.Resources, nil
}

// ListAcls Returns a list of all principals that have ACLs defined.
func (c *Client) ListAcls(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, ACLRequestTimeout)
	defer cancel()

	// Check if the broker supports the DescribeACLs API (key 29).
	// Strimzi clusters without an authorizer configured will not advertise this API.
	supported, _, err := c.CheckAPISupport(ctx, 29)
	if err != nil {
		return nil, fmt.Errorf("failed to check ACL API support: %w", err)
	}
	if !supported {
		return nil, fmt.Errorf("broker does not support the DescribeACLs API (key 29). " +
			"This typically means no ACL authorizer is configured on the Kafka cluster. " +
			"For Strimzi, set 'authorization' in the Kafka custom resource (e.g., type: simple)")
	}

	// Create a request with ALL filters set to ANY (1) to match everything.
	// The default value 0 means UNKNOWN which brokers reject.
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceTypeAny
	req.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	req.Operation = kmsg.ACLOperationAny
	req.PermissionType = kmsg.ACLPermissionTypeAny
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACLs (timeout=%v): %w", ACLRequestTimeout, err)
	}

	if resp.ErrorCode != 0 {
		return nil, formatACLError("list ACLs", resp.ErrorCode)
	}

	// Create a map to store unique principals
	principalSet := make(map[string]struct{})

	// Process each ACL result
	for _, resource := range resp.Resources {
		for _, acl := range resource.ACLs {
			// Include all principals, not just those with "User:" prefix
			principalSet[acl.Principal] = struct{}{}
		}
	}

	// Convert the set to a slice
	principals := make([]string, 0, len(principalSet))
	for principal := range principalSet {
		principals = append(principals, principal)
	}

	return principals, nil
}

// formatACLError translates Kafka error codes into human-readable error messages
// for ACL operations.
func formatACLError(operation string, code int16) error {
	switch code {
	case 7:
		return nil // Metadata still updating, treat as success
	case 8:
		return fmt.Errorf("failed to %s: security is disabled on the broker (no authorizer configured). "+
			"For Strimzi, set 'authorization' in the Kafka custom resource (e.g., type: simple)", operation)
	case 31:
		return fmt.Errorf("failed to %s: cluster authorization failed. "+
			"The authenticated user does not have permission to describe ACLs. "+
			"Ensure the KafkaUser has 'Describe' permission on the 'Cluster' resource", operation)
	case 87:
		return fmt.Errorf("failed to %s: invalid resource type or name", operation)
	case 88:
		return fmt.Errorf("failed to %s: invalid principal format", operation)
	default:
		return fmt.Errorf("failed to %s: error code %d", operation, code)
	}
}

// handleACLCreateError Processes error codes from ACL creation requests
// and returns appropriate error messages.
func handleACLCreateError(resp *kmsg.CreateACLsResponse) error {
	if len(resp.Results) > 0 && resp.Results[0].ErrorCode != 0 {
		return formatACLError("create ACL", resp.Results[0].ErrorCode)
	}
	return nil
}
