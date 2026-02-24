package kafka

import (
	"context"
	"fmt"
	"strconv"
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
		switch resp.Results[0].ErrorCode {
		case 7:
			// Error code 7 during deletion seems to be returned when the operation is successful
			// but the metadata is still being updated
			return nil
		default:
			return fmt.Errorf("failed to delete ACL: error code %v", resp.Results[0].ErrorCode)
		}
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

// GetAcl Retrieves ACL entries matching the specified resource type, name, and principal.
// Returns a list of ACL resources that match the criteria.
func (c *Client) GetAcl(ctx context.Context, resourceType, resourceName, principal string) ([]kmsg.DescribeACLsResponseResource, error) {
	ctx, cancel := context.WithTimeout(ctx, ACLRequestTimeout)
	defer cancel()

	resourceTypeInt, err := strconv.Atoi(resourceType)
	if err != nil {
		return nil, fmt.Errorf("invalid resource type: %w", err)
	}

	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceType(resourceTypeInt)
	req.ResourceName = &resourceName
	req.Principal = &principal
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get ACL (timeout=%v): %w", ACLRequestTimeout, err)
	}
	if resp.ErrorCode != 0 {
		return nil, fmt.Errorf("failed to get ACL: %v", resp.ErrorCode)
	}
	if len(resp.Resources) == 0 {
		return nil, fmt.Errorf("no ACLs found for resource type %s, name %s, and principal %s", resourceType, resourceName, principal)
	}
	return resp.Resources, nil
}

// ListAcls Returns a list of all principals that have ACLs defined.
func (c *Client) ListAcls(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, ACLRequestTimeout)
	defer cancel()

	// Create a request with no filters to get all ACLs
	req := kmsg.NewPtrDescribeACLsRequest()
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACLs (timeout=%v): %w", ACLRequestTimeout, err)
	}

	if resp.ErrorCode != 0 {
		return nil, fmt.Errorf("failed to list ACLs: error code %v", resp.ErrorCode)
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

// handleACLCreateError Processes error codes from ACL creation requests
// and returns appropriate error messages.
func handleACLCreateError(resp *kmsg.CreateACLsResponse) error {
	if len(resp.Results) > 0 && resp.Results[0].ErrorCode != 0 {
		switch resp.Results[0].ErrorCode {
		case 7:
			return nil
		case 87:
			return fmt.Errorf("invalid resource type or name")
		case 88:
			return fmt.Errorf("invalid principal format")
		default:
			return fmt.Errorf("failed to create ACL: error code %v", resp.Results[0].ErrorCode)
		}
	}
	return nil
}
