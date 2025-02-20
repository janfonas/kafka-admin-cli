package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

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

func (c *Client) ListAcls(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req := &kmsg.DescribeACLsRequest{}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACLs: %w", err)
	}

	if resp.ErrorCode != 0 {
		return nil, fmt.Errorf("failed to list ACLs: error code %v", resp.ErrorCode)
	}

	principalSet := make(map[string]struct{})
	for _, resource := range resp.Resources {
		for _, acl := range resource.ACLs {
			if strings.HasPrefix(acl.Principal, "User:") {
				principal := strings.TrimPrefix(acl.Principal, "User:")
				principalSet[principal] = struct{}{}
			}
		}
	}

	var principals []string
	for principal := range principalSet {
		principals = append(principals, principal)
	}

	return principals, nil
}

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
