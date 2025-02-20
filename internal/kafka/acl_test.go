package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestACLErrorHandling(t *testing.T) {
	tests := []struct {
		name      string
		errorCode int16
		wantError bool
		errorMsg  string
	}{
		{
			name:      "success",
			errorCode: 0,
			wantError: false,
		},
		{
			name:      "metadata update",
			errorCode: 7,
			wantError: false,
		},
		{
			name:      "invalid resource",
			errorCode: 87,
			wantError: true,
			errorMsg:  "invalid resource type or name",
		},
		{
			name:      "invalid principal",
			errorCode: 88,
			wantError: true,
			errorMsg:  "invalid principal format",
		},
		{
			name:      "unknown error",
			errorCode: 50,
			wantError: true,
			errorMsg:  "failed to create ACL: error code 50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &kmsg.CreateACLsResponse{
				Results: []kmsg.CreateACLsResponseResult{
					{
						ErrorCode: tt.errorCode,
					},
				},
			}

			err := handleACLCreateError(resp)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestModifyACL(t *testing.T) {
	tests := []struct {
		name          string
		resourceType  string
		resourceName  string
		principal     string
		host          string
		operation     string
		permission    string
		newPermission string
		deleteError   int16
		createError   int16
		wantError     bool
		errorMsg      string
	}{
		{
			name:          "success",
			resourceType:  "1", // TOPIC
			resourceName:  "test-topic",
			principal:     "User:alice",
			host:          "*",
			operation:     "2", // READ
			permission:    "3", // ALLOW
			newPermission: "4", // DENY
			deleteError:   0,
			createError:   0,
			wantError:     false,
		},
		{
			name:          "delete error",
			resourceType:  "1",
			resourceName:  "test-topic",
			principal:     "User:alice",
			host:          "*",
			operation:     "2",
			permission:    "3",
			newPermission: "4",
			deleteError:   87,
			createError:   0,
			wantError:     true,
			errorMsg:      "failed to delete existing ACL: failed to delete ACL: error code 87",
		},
		{
			name:          "create error",
			resourceType:  "1",
			resourceName:  "test-topic",
			principal:     "User:alice",
			host:          "*",
			operation:     "2",
			permission:    "3",
			newPermission: "4",
			deleteError:   0,
			createError:   88,
			wantError:     true,
			errorMsg:      "failed to create new ACL: invalid principal format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockClient(
				&kmsg.DeleteACLsResponse{
					Results: []kmsg.DeleteACLsResponseResult{
						{
							ErrorCode: tt.deleteError,
						},
					},
				},
				&kmsg.CreateACLsResponse{
					Results: []kmsg.CreateACLsResponseResult{
						{
							ErrorCode: tt.createError,
						},
					},
				},
			)

			client := NewClientWithMock(mockClient)

			err := client.ModifyAcl(context.Background(), tt.resourceType, tt.resourceName, tt.principal, tt.host, tt.operation, tt.permission, tt.newPermission)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestListACLs(t *testing.T) {
	tests := []struct {
		name           string
		errorCode      int16
		resources      []kmsg.DescribeACLsResponseResource
		wantError      bool
		errorMsg       string
		wantPrincipals []string
	}{
		{
			name:      "success",
			errorCode: 0,
			resources: []kmsg.DescribeACLsResponseResource{
				{
					ResourceType: kmsg.ACLResourceTypeTopic,
					ResourceName: "test-topic",
					ACLs: []kmsg.DescribeACLsResponseResourceACL{
						{Principal: "User:alice"},
						{Principal: "User:bob"},
					},
				},
				{
					ResourceType: kmsg.ACLResourceTypeGroup,
					ResourceName: "test-group",
					ACLs: []kmsg.DescribeACLsResponseResourceACL{
						{Principal: "User:alice"},
						{Principal: "User:charlie"},
					},
				},
			},
			wantError:      false,
			wantPrincipals: []string{"User:alice", "User:bob", "User:charlie"},
		},
		{
			name:      "error response",
			errorCode: 50,
			resources: nil,
			wantError: true,
			errorMsg:  "failed to list ACLs: error code 50",
		},
		{
			name:           "no ACLs",
			errorCode:      0,
			resources:      []kmsg.DescribeACLsResponseResource{},
			wantError:      false,
			wantPrincipals: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockClient(
				&kmsg.DescribeACLsResponse{
					ErrorCode: tt.errorCode,
					Resources: tt.resources,
				},
			)

			client := NewClientWithMock(mockClient)

			principals, err := client.ListAcls(context.Background())
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// Check if we got all expected principals
				principalMap := make(map[string]struct{})
				for _, p := range principals {
					principalMap[p] = struct{}{}
				}
				for _, want := range tt.wantPrincipals {
					if _, ok := principalMap[want]; !ok {
						t.Errorf("missing expected principal %q", want)
					}
				}

				// Check if we got any unexpected principals
				if len(principals) != len(tt.wantPrincipals) {
					t.Errorf("got %d principals, want %d", len(principals), len(tt.wantPrincipals))
				}
			}
		})
	}
}
