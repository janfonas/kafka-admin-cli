package cmd

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestFormatACLStrimziTLSPrincipals(t *testing.T) {
	principals := []string{"User:CN=nt-ibm-es-kafka-user", "User:alice", "User:CN=service-b"}
	resources := aclResources(append(principals, principals[0])...)
	resources[0].ACLs[3].Operation = kmsg.ACLOperationWrite
	group := aclResources(principals[0])[0]
	group.ResourceType = kmsg.ACLResourceTypeGroup
	group.ResourceName = "UPPER.group"
	resources = append(resources, group)

	var out bytes.Buffer
	if err := formatACLStrimzi(&out, resources, aclExportOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(out.String(), "\n---\n"); got != len(principals)-1 {
		t.Errorf("expected %d document separators, got %d", len(principals)-1, got)
	}
	documents := decodeStrimziDocuments[strimziUserSpec](t, out.Bytes())
	if len(documents) != len(principals) {
		t.Fatalf("expected %d documents, got %d", len(principals), len(documents))
	}
	for i, document := range documents {
		wantName := []string{"nt-ibm-es-kafka-user", "alice", "service-b"}[i]
		if document.Metadata.Name != wantName || document.Kind != "KafkaUser" {
			t.Errorf("unexpected manifest: %+v", document)
		}
		effectivePrincipal := "User:"
		if i == 1 {
			if document.Spec.Authentication != nil {
				t.Error("plain user export must not configure authentication")
			}
		} else {
			if document.Spec.Authentication == nil || document.Spec.Authentication.Type != "tls-external" {
				t.Fatalf("TLS principal must use tls-external: %+v", document.Spec)
			}
			effectivePrincipal += "CN="
		}
		effectivePrincipal += document.Metadata.Name
		if effectivePrincipal != principals[i] {
			t.Errorf("Kafka principal changed: got %q, want %q", effectivePrincipal, principals[i])
		}
	}
	acls := documents[0].Spec.Authorization.ACLs
	if len(acls) != 2 {
		t.Fatalf("expected two ACL resources for TLS principal, got %d", len(acls))
	}
	if !reflect.DeepEqual(acls[0].Operations, []string{"Read", "Write"}) {
		t.Errorf("unexpected TLS ACL operations: %v", acls[0].Operations)
	}
	if acls[1].Resource.Type != "group" || acls[1].Resource.Name == nil || *acls[1].Resource.Name != "UPPER.group" {
		t.Errorf("ACL resource identity must not be sanitized: %+v", acls[1].Resource)
	}
}

func TestFormatACLStrimziRejectsUnrepresentableTLSPrincipals(t *testing.T) {
	for _, principal := range []string{
		"User:CN=",
		"User:CN=Alice",
		"User:CN=alice_admin",
		"User:CN=alice,OU=admins",
		"User:OU=admins,CN=alice",
		"User:CN=alice+UID=123",
		"User:CN=alice\\,bob",
		"User:CN=\\61lice",
		"User:CN=\"alice\"",
		"User:cn=alice",
		"User:CN= alice",
		"User:CN=alice ",
		"User:CN=alice\n---\napiVersion: v1\nkind: ConfigMap",
		"User:CN=alice\x00",
		"User:CN=" + strings.Repeat("a", maxKubernetesNameLength+1),
	} {
		t.Run(principal, func(t *testing.T) {
			var out bytes.Buffer
			err := formatACLStrimzi(&out, aclResources("User:valid", principal), aclExportOptions{})
			if err == nil || !strings.Contains(err.Error(), "cannot export principal") {
				t.Fatalf("expected principal error, got %v", err)
			}
			if out.Len() != 0 {
				t.Fatalf("failed export wrote partial manifests: %s", &out)
			}
		})
	}
}

func TestFormatACLStrimziRejectsIdentityCollisions(t *testing.T) {
	for _, principals := range [][]string{
		{"User:alice", "User:CN=alice"},
		{"User:CN=alice", "User:alice"},
	} {
		t.Run(strings.Join(principals, ","), func(t *testing.T) {
			var out bytes.Buffer
			err := formatACLStrimzi(&out, aclResources(principals...), aclExportOptions{})
			if err == nil || !strings.Contains(err.Error(), "both map to KafkaUser") {
				t.Fatalf("expected collision error, got %v", err)
			}
			if out.Len() != 0 {
				t.Fatalf("colliding users wrote partial manifests: %s", &out)
			}
		})
	}
}
