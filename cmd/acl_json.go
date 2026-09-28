package cmd

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// External tools consume this schema; bump the version on any incompatible change.
const (
	aclExportKind          = "KafkaACLExport"
	aclExportSchemaVersion = 1
)

type aclExport struct {
	Kind          string          `json:"kind"`
	SchemaVersion int             `json:"schemaVersion"`
	ClusterID     string          `json:"clusterId"`
	CapturedAt    time.Time       `json:"capturedAt"`
	Filter        aclExportFilter `json:"filter"`
	Bindings      []aclBinding    `json:"bindings"`
}

// An empty filter means the export is a complete listing.
type aclExportFilter struct {
	ResourceType string `json:"resourceType,omitempty"`
	ResourceName string `json:"resourceName,omitempty"`
	Principal    string `json:"principal,omitempty"`
}

type aclBinding struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resourceType"`
	ResourceName string `json:"resourceName"`
	PatternType  string `json:"patternType"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"`
}

type aclExportSource interface {
	DescribeAcls(ctx context.Context, resourceType, resourceName, principal string) ([]kmsg.DescribeACLsResponseResource, error)
	ClusterID(ctx context.Context) (string, error)
}

func exportACLJSON(ctx context.Context, w io.Writer, source aclExportSource, resourceType, resourceName, principal string) error {
	filter, err := newACLExportFilter(resourceType, resourceName, principal)
	if err != nil {
		return err
	}
	capturedAt := time.Now().UTC()
	resources, err := source.DescribeAcls(ctx, resourceType, resourceName, principal)
	if err != nil {
		return err
	}
	clusterID, err := source.ClusterID(ctx)
	if err != nil {
		return err
	}
	export, err := newACLExport(clusterID, capturedAt, filter, resources)
	if err != nil {
		return err
	}
	return writeACLJSON(w, export)
}

func newACLExportFilter(resourceType, resourceName, principal string) (aclExportFilter, error) {
	filter := aclExportFilter{ResourceName: resourceName, Principal: principal}
	if resourceType != "" {
		value, err := strconv.Atoi(resourceType)
		if err != nil {
			return aclExportFilter{}, fmt.Errorf("invalid resource type: %w", err)
		}
		filter.ResourceType = kmsg.ACLResourceType(value).String()
		if filter.ResourceType == "UNKNOWN" {
			return aclExportFilter{}, fmt.Errorf("invalid resource type %d", value)
		}
	}
	return filter, nil
}

func newACLExport(clusterID string, capturedAt time.Time, filter aclExportFilter, resources []kmsg.DescribeACLsResponseResource) (aclExport, error) {
	count := 0
	for _, resource := range resources {
		count += len(resource.ACLs)
	}
	bindings := make([]aclBinding, 0, count)
	for _, resource := range resources {
		resourceType := resource.ResourceType.String()
		if resourceType == "UNKNOWN" || resourceType == "ANY" {
			return aclExport{}, fmt.Errorf("cannot export ACLs for resource %q: broker returned non-concrete resource type %d", resource.ResourceName, resource.ResourceType)
		}
		patternType := resource.ResourcePatternType.String()
		if patternType != "LITERAL" && patternType != "PREFIXED" {
			return aclExport{}, fmt.Errorf("cannot export ACLs for resource %q: broker returned non-concrete pattern type %d", resource.ResourceName, resource.ResourcePatternType)
		}
		for _, acl := range resource.ACLs {
			operation := acl.Operation.String()
			if operation == "UNKNOWN" || operation == "ANY" {
				return aclExport{}, fmt.Errorf("cannot export ACL for principal %q on %q: broker returned non-concrete operation %d", acl.Principal, resource.ResourceName, acl.Operation)
			}
			permission := acl.PermissionType.String()
			if permission != "ALLOW" && permission != "DENY" {
				return aclExport{}, fmt.Errorf("cannot export ACL for principal %q on %q: broker returned non-concrete permission %d", acl.Principal, resource.ResourceName, acl.PermissionType)
			}
			bindings = append(bindings, aclBinding{
				Principal:    acl.Principal,
				Host:         acl.Host,
				ResourceType: resourceType,
				ResourceName: resource.ResourceName,
				PatternType:  patternType,
				Operation:    operation,
				Permission:   permission,
			})
		}
	}
	slices.SortFunc(bindings, compareACLBindings)
	for i := 1; i < len(bindings); i++ {
		if bindings[i] == bindings[i-1] {
			return aclExport{}, fmt.Errorf("cannot export ACLs: broker returned duplicate binding %+v", bindings[i])
		}
	}
	return aclExport{
		Kind:          aclExportKind,
		SchemaVersion: aclExportSchemaVersion,
		ClusterID:     clusterID,
		CapturedAt:    capturedAt,
		Filter:        filter,
		Bindings:      bindings,
	}, nil
}

func compareACLBindings(a, b aclBinding) int {
	return cmp.Or(
		cmp.Compare(a.Principal, b.Principal),
		cmp.Compare(a.ResourceType, b.ResourceType),
		cmp.Compare(a.ResourceName, b.ResourceName),
		cmp.Compare(a.PatternType, b.PatternType),
		cmp.Compare(a.Host, b.Host),
		cmp.Compare(a.Operation, b.Operation),
		cmp.Compare(a.Permission, b.Permission),
	)
}

func writeACLJSON(w io.Writer, export aclExport) error {
	data, err := json.MarshalIndent(export, "", "  ")
	if err != nil {
		return fmt.Errorf("encode ACL export: %w", err)
	}
	if _, err := w.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write ACL export: %w", err)
	}
	return nil
}
