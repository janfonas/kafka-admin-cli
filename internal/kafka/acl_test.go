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
