package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestACLErrorHandling(t *testing.T) {
	tests := []struct {
		name string
		code int16
		want error
	}{
		{name: "success", code: 0},
		{name: "request timed out is a failure", code: kerr.RequestTimedOut.Code, want: kerr.RequestTimedOut},
		{name: "broker not available", code: kerr.BrokerNotAvailable.Code, want: kerr.BrokerNotAvailable},
		{name: "security disabled", code: kerr.SecurityDisabled.Code, want: kerr.SecurityDisabled},
		{name: "cluster authorization", code: kerr.ClusterAuthorizationFailed.Code, want: kerr.ClusterAuthorizationFailed},
		{name: "unknown code", code: 32000, want: kerr.UnknownServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &kmsg.CreateACLsResponse{
				Results: []kmsg.CreateACLsResponseResult{{ErrorCode: tt.code}},
			}

			err := handleACLCreateError(resp)
			if tt.want == nil {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}
			if !strings.HasPrefix(err.Error(), "failed to create ACL (error code ") {
				t.Errorf("missing operation and code: %q", err)
			}
		})
	}
}

func TestACLErrorIncludesBrokerMessage(t *testing.T) {
	message := "principal type is not supported"
	err := aclError("create ACL", kerr.InvalidRequest.Code, &message)
	if !errors.Is(err, kerr.InvalidRequest) || !strings.Contains(err.Error(), `broker message "principal type is not supported"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDescribeACLsFailuresAreNeverEmptySuccess(t *testing.T) {
	for _, code := range []int16{kerr.RequestTimedOut.Code, kerr.SecurityDisabled.Code, kerr.ClusterAuthorizationFailed.Code} {
		client := NewClientWithMock(newMockClient(&kmsg.DescribeACLsResponse{ErrorCode: code}))
		resources, err := client.DescribeAcls(context.Background(), "", "", "")
		if !errors.Is(err, kerr.ErrorForCode(code)) || resources != nil {
			t.Errorf("DescribeAcls code %d: got %v, %v", code, resources, err)
		}
		if _, err := client.GetAcl(context.Background(), "", "", ""); !errors.Is(err, kerr.ErrorForCode(code)) {
			t.Errorf("GetAcl code %d: got %v", code, err)
		}
		if _, err := client.ListAcls(context.Background()); !errors.Is(err, kerr.ErrorForCode(code)) {
			t.Errorf("ListAcls code %d: got %v", code, err)
		}
	}
}

func TestDescribeACLsEmptyListingIsValid(t *testing.T) {
	client := NewClientWithMock(newMockClient(&kmsg.DescribeACLsResponse{}))
	resources, err := client.DescribeAcls(context.Background(), "", "", "User:nobody")
	if err != nil || len(resources) != 0 {
		t.Fatalf("expected empty listing, got %v, %v", resources, err)
	}
	if _, err := client.GetAcl(context.Background(), "", "", "User:nobody"); err == nil || !strings.Contains(err.Error(), "no ACLs found") {
		t.Fatalf("GetAcl must still report an empty interactive lookup, got %v", err)
	}
}

func TestClusterID(t *testing.T) {
	id := "Hm2pQ4rXS7yN0vB8kLd3aw"
	for _, tt := range []struct {
		name    string
		id      *string
		wantErr bool
	}{
		{name: "present", id: &id},
		{name: "missing", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := NewClientWithMock(newMockClient(&kmsg.MetadataResponse{ClusterID: tt.id}))
			got, err := client.ClusterID(context.Background())
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil || got != id {
				t.Fatalf("got %q, %v", got, err)
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
			errorMsg:      "failed to delete existing ACL: failed to delete ACL (error code 87)",
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
			errorMsg:      "failed to create new ACL: failed to create ACL (error code 88)",
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
					t.Fatal("expected error, got nil")
				}
				if !strings.HasPrefix(err.Error(), tt.errorMsg) {
					t.Errorf("expected error prefix %q, got %q", tt.errorMsg, err.Error())
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
			errorMsg:  "failed to list ACLs (error code 50)",
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
					t.Fatal("expected error, got nil")
				}
				if !strings.HasPrefix(err.Error(), tt.errorMsg) {
					t.Errorf("expected error prefix %q, got %q", tt.errorMsg, err.Error())
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
