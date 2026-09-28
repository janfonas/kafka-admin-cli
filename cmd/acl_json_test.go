package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

var exportCapturedAt = time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)

const exportClusterID = "Hm2pQ4rXS7yN0vB8kLd3aw"

func aclEntry(principal, host string, operation kmsg.ACLOperation, permission kmsg.ACLPermissionType) kmsg.DescribeACLsResponseResourceACL {
	return kmsg.DescribeACLsResponseResourceACL{Principal: principal, Host: host, Operation: operation, PermissionType: permission}
}

// sharedExportResources mirrors testdata/acl-export-v1.json in broker response shape and order.
func sharedExportResources() []kmsg.DescribeACLsResponseResource {
	allow, deny := kmsg.ACLPermissionTypeAllow, kmsg.ACLPermissionTypeDeny
	return []kmsg.DescribeACLsResponseResource{
		{ResourceType: kmsg.ACLResourceTypeTopic, ResourceName: "payments", ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral, ACLs: []kmsg.DescribeACLsResponseResourceACL{
			aclEntry("User:alice", "10.0.0.5", kmsg.ACLOperationWrite, allow),
			aclEntry("User:alice", "*", kmsg.ACLOperationDelete, deny),
		}},
		{ResourceType: kmsg.ACLResourceTypeTopic, ResourceName: "orders", ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral, ACLs: []kmsg.DescribeACLsResponseResourceACL{
			aclEntry("User:CN=orders-app", "*", kmsg.ACLOperationRead, allow),
			aclEntry("User:CN=orders-app", "*", kmsg.ACLOperationDescribe, allow),
		}},
		{ResourceType: kmsg.ACLResourceTypeCluster, ResourceName: "kafka-cluster", ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral, ACLs: []kmsg.DescribeACLsResponseResourceACL{
			aclEntry("User:admin", "*", kmsg.ACLOperationAlter, allow),
		}},
		{ResourceType: kmsg.ACLResourceTypeTransactionalId, ResourceName: "payments-tx", ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral, ACLs: []kmsg.DescribeACLsResponseResourceACL{
			aclEntry("User:alice", "*", kmsg.ACLOperationWrite, allow),
		}},
		{ResourceType: kmsg.ACLResourceTypeTopic, ResourceName: "public-", ResourcePatternType: kmsg.ACLResourcePatternTypePrefixed, ACLs: []kmsg.DescribeACLsResponseResourceACL{
			aclEntry("User:*", "*", kmsg.ACLOperationRead, allow),
		}},
		{ResourceType: kmsg.ACLResourceTypeGroup, ResourceName: "orders-", ResourcePatternType: kmsg.ACLResourcePatternTypePrefixed, ACLs: []kmsg.DescribeACLsResponseResourceACL{
			aclEntry("User:CN=orders-app", "*", kmsg.ACLOperationRead, allow),
		}},
	}
}

func TestACLExportMatchesSharedFixtures(t *testing.T) {
	for _, tt := range []struct {
		fixture   string
		filter    aclExportFilter
		resources []kmsg.DescribeACLsResponseResource
	}{
		{fixture: "testdata/acl-export-v1.json", resources: sharedExportResources()},
		{fixture: "testdata/acl-export-v1-filtered-empty.json", filter: aclExportFilter{Principal: "User:nobody"}},
	} {
		t.Run(tt.fixture, func(t *testing.T) {
			export, err := newACLExport(exportClusterID, exportCapturedAt, tt.filter, tt.resources)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := writeACLJSON(&out, export); err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(tt.fixture)
			if err != nil {
				t.Fatal(err)
			}
			if out.String() != string(want) {
				t.Fatalf("export differs from shared fixture %s:\n%s", tt.fixture, out.String())
			}
		})
	}
}

func TestACLExportRejectsNonConcreteValues(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*kmsg.DescribeACLsResponseResource)
		want   string
	}{
		{"any resource type", func(r *kmsg.DescribeACLsResponseResource) { r.ResourceType = kmsg.ACLResourceTypeAny }, "resource type"},
		{"unknown resource type", func(r *kmsg.DescribeACLsResponseResource) { r.ResourceType = 0 }, "resource type"},
		{"match pattern", func(r *kmsg.DescribeACLsResponseResource) { r.ResourcePatternType = kmsg.ACLResourcePatternTypeMatch }, "pattern type"},
		{"unknown operation", func(r *kmsg.DescribeACLsResponseResource) { r.ACLs[0].Operation = 0 }, "operation"},
		{"any permission", func(r *kmsg.DescribeACLsResponseResource) { r.ACLs[0].PermissionType = kmsg.ACLPermissionTypeAny }, "permission"},
		{"duplicate binding", func(r *kmsg.DescribeACLsResponseResource) { r.ACLs = append(r.ACLs, r.ACLs[0]) }, "duplicate"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resources := sharedExportResources()
			tt.mutate(&resources[0])
			if _, err := newACLExport(exportClusterID, exportCapturedAt, aclExportFilter{}, resources); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

type fakeACLSource struct {
	resources []kmsg.DescribeACLsResponseResource
	err       error
	gotFilter [3]string
}

func (f *fakeACLSource) DescribeAcls(_ context.Context, resourceType, resourceName, principal string) ([]kmsg.DescribeACLsResponseResource, error) {
	f.gotFilter = [3]string{resourceType, resourceName, principal}
	return f.resources, f.err
}

func (f *fakeACLSource) ClusterID(context.Context) (string, error) { return exportClusterID, nil }

func TestExportACLJSONRecordsFilter(t *testing.T) {
	source := &fakeACLSource{}
	var out bytes.Buffer
	if err := exportACLJSON(context.Background(), &out, source, "2", "orders", "User:alice"); err != nil {
		t.Fatal(err)
	}
	if source.gotFilter != [3]string{"2", "orders", "User:alice"} {
		t.Fatalf("filter not passed to broker: %v", source.gotFilter)
	}
	if !strings.Contains(out.String(), `"resourceType": "TOPIC"`) || !strings.Contains(out.String(), `"bindings": []`) {
		t.Fatalf("filtered empty export not recorded: %s", out.String())
	}
}

func TestExportACLJSONFailuresWriteNothing(t *testing.T) {
	for _, tt := range []struct {
		name         string
		resourceType string
		source       *fakeACLSource
	}{
		{"broker error", "", &fakeACLSource{err: errors.New("describe failed")}},
		{"invalid filter", "topic", &fakeACLSource{}},
		{"unknown filter value", "99", &fakeACLSource{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := exportACLJSON(context.Background(), &out, tt.source, tt.resourceType, "", ""); err == nil {
				t.Fatal("expected error")
			}
			if out.Len() != 0 {
				t.Fatalf("failed export wrote output: %s", out.String())
			}
		})
	}
}

func TestFormatACLStrimziRejectsUnsupportedValues(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*kmsg.DescribeACLsResponseResource)
	}{
		{"delegation token", func(r *kmsg.DescribeACLsResponseResource) { r.ResourceType = kmsg.ACLResourceTypeDelegationToken }},
		{"user resource", func(r *kmsg.DescribeACLsResponseResource) { r.ResourceType = kmsg.ACLResourceTypeUser }},
		{"match pattern", func(r *kmsg.DescribeACLsResponseResource) { r.ResourcePatternType = kmsg.ACLResourcePatternTypeMatch }},
		{"create tokens", func(r *kmsg.DescribeACLsResponseResource) { r.ACLs[0].Operation = kmsg.ACLOperationCreateTokens }},
		{"unknown permission", func(r *kmsg.DescribeACLsResponseResource) { r.ACLs[0].PermissionType = 0 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resources := aclResources("User:alice")
			tt.mutate(&resources[0])
			var out bytes.Buffer
			err := formatACLStrimzi(&out, resources, aclExportOptions{})
			if err == nil || !strings.Contains(err.Error(), "not supported by Strimzi KafkaUser") {
				t.Fatalf("expected unsupported-value error, got %v", err)
			}
			if out.Len() != 0 {
				t.Fatalf("unsupported value wrote a manifest: %s", out.String())
			}
		})
	}
}

func TestACLOutputFormatValidation(t *testing.T) {
	for _, format := range aclOutputFormats {
		if err := validateACLOutputFormat(format); err != nil {
			t.Errorf("%s rejected: %v", format, err)
		}
	}
	if err := validateACLOutputFormat("yaml"); err == nil {
		t.Fatal("unknown ACL output format accepted")
	}
}
