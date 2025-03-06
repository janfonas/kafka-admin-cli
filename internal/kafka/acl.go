package kafka

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// CreateAcl Creates a new Access Control List (ACL) entry in Kafka.
// Parameters include resource type (e.g., topic), resource name, principal (user),
// host, operation (e.g., read, write), and permission type (allow/deny).
func (c *Client) CreateAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string) error {
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

	req := &kmsg.CreateACLsRequest{
		Creations: []kmsg.CreateACLsRequestCreation{
			{
				ResourceType:   kmsg.ACLResourceType(resourceTypeInt),
				ResourceName:   resourceName,
				Principal:      principal,
				Host:           host,
				Operation:      kmsg.ACLOperation(operationInt),
				PermissionType: kmsg.ACLPermissionType(permissionInt),
			},
		},
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to create ACL: %w", err)
	}
	return handleACLCreateError(resp)
}

// DeleteAcl Removes an existing ACL entry from Kafka.
// The parameters must match exactly with an existing ACL entry for it to be deleted.
func (c *Client) DeleteAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string) error {
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

	req := &kmsg.DeleteACLsRequest{
		Filters: []kmsg.DeleteACLsRequestFilter{
			{
				ResourceType:   kmsg.ACLResourceType(resourceTypeInt),
				ResourceName:   &resourceName,
				Principal:      &principal,
				Host:           &host,
				Operation:      kmsg.ACLOperation(operationInt),
				PermissionType: kmsg.ACLPermissionType(permissionInt),
			},
		},
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete ACL: %w", err)
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
	resourceTypeInt, err := strconv.Atoi(resourceType)
	if err != nil {
		return nil, fmt.Errorf("invalid resource type: %w", err)
	}

	req := &kmsg.DescribeACLsRequest{
		ResourceType: kmsg.ACLResourceType(resourceTypeInt),
		ResourceName: &resourceName,
		Principal:    &principal,
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get ACL: %w", err)
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
	if c.debug {
		fmt.Println("DEBUG: Creating ACL list request")
	}

	// Create a request with specific resource type for topics
	// This is more specific than using ANY and might be better handled
	req := &kmsg.DescribeACLsRequest{
		ResourceType: kmsg.ACLResourceTypeTopic,
		ResourceName: nil,
		Principal:    nil,
		Host:         nil,
	}

	if c.debug {
		fmt.Printf("DEBUG: Request details:\n")
		fmt.Printf("  ResourceType: %v\n", req.ResourceType)
		fmt.Printf("  ResourceName: %v\n", req.ResourceName)
		fmt.Printf("  Principal: %v\n", req.Principal)
		fmt.Printf("  Host: %v\n", req.Host)
		fmt.Printf("  Operation: %v\n", req.Operation)
		fmt.Printf("  PermissionType: %v\n", req.PermissionType)
	}

	if c.debug {
		fmt.Println("DEBUG: Attempting to send request to broker")
	}

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		if c.debug {
			fmt.Printf("DEBUG: Request failed with error: %v\n", err)
		}
		return nil, fmt.Errorf("failed to list ACLs: %w", err)
	}

	if c.debug {
		fmt.Printf("DEBUG: Received response with error code: %v\n", resp.ErrorCode)
	}

	if resp.ErrorCode != 0 {
		if c.debug {
			fmt.Printf("DEBUG: Response indicates error: code=%v\n", resp.ErrorCode)
		}
		return nil, fmt.Errorf("failed to list ACLs: error code %v", resp.ErrorCode)
	}

	if c.debug {
		fmt.Printf("DEBUG: Processing response with %d resources\n", len(resp.Resources))
	}

	// Create a map to store unique principals
	principalSet := make(map[string]struct{})

	// Process each ACL resource
	for i, resource := range resp.Resources {
		if c.debug {
			fmt.Printf("DEBUG: Processing resource %d with %d ACLs\n", i+1, len(resource.ACLs))
		}
		for _, acl := range resource.ACLs {
			if acl.Principal != "" {
				if c.debug {
					fmt.Printf("DEBUG: Found principal: %s\n", acl.Principal)
				}
				principalSet[acl.Principal] = struct{}{}
			}
		}
	}

	// Convert the set to a slice
	principals := make([]string, 0, len(principalSet))
	for principal := range principalSet {
		principals = append(principals, principal)
	}

	if c.debug {
		fmt.Printf("DEBUG: Returning %d unique principals\n", len(principals))
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
