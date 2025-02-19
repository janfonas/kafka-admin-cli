package acl

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type AclOperations struct {
	client *kgo.Client
}

func NewAclOperations(client *kgo.Client) *AclOperations {
	return &AclOperations{client: client}
}

func (a *AclOperations) CreateAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string) error {
	_, err := a.client.CreateACLs(ctx, []kgo.ACL{
		{
			ResourceType: resourceType,
			ResourceName: resourceName,
			Principal:    principal,
			Host:         host,
			Operation:    operation,
			Permission:   permission,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create ACL: %w", err)
	}
	return nil
}

func (a *AclOperations) DeleteAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string) error {
	_, err := a.client.DeleteACLs(ctx, []kgo.ACL{
		{
			ResourceType: resourceType,
			ResourceName: resourceName,
			Principal:    principal,
			Host:         host,
			Operation:    operation,
			Permission:   permission,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to delete ACL: %w", err)
	}
	return nil
}

func (a *AclOperations) ListAcls(ctx context.Context) ([]kgo.ACL, error) {
	acls, err := a.client.ListACLs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACLs: %w", err)
	}
	return acls, nil
}
