package cmd

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/janfonas/kafka-admin-cli/internal/kafka"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.yaml.in/yaml/v3"
)

func aclResources(principals ...string) []kmsg.DescribeACLsResponseResource {
	resource := kmsg.DescribeACLsResponseResource{
		ResourceType:        kmsg.ACLResourceTypeTopic,
		ResourceName:        "orders",
		ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral,
	}
	for _, principal := range principals {
		resource.ACLs = append(resource.ACLs, kmsg.DescribeACLsResponseResourceACL{
			Principal:      principal,
			Host:           "*",
			Operation:      kmsg.ACLOperationRead,
			PermissionType: kmsg.ACLPermissionTypeAllow,
		})
	}
	return []kmsg.DescribeACLsResponseResource{resource}
}

func decodeStrimziDocuments[T any](t *testing.T, data []byte) []strimziManifest[T] {
	t.Helper()
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var documents []strimziManifest[T]
	for {
		var document strimziManifest[T]
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			return documents
		}
		if err != nil {
			t.Fatalf("decode manifest: %v\n%s", err, data)
		}
		documents = append(documents, document)
	}
}

func TestFormatACLStrimzi(t *testing.T) {
	resources := aclResources("User:alice", "User:alice", "User:bob", "User:alice")
	resources[0].ACLs[1].Operation = kmsg.ACLOperationWrite
	resources[0].ACLs[3].PermissionType = kmsg.ACLPermissionTypeDeny
	resources[0].ACLs[3].Host = "192.0.2.1"
	var out bytes.Buffer
	if err := formatACLStrimzi(&out, resources); err != nil {
		t.Fatal(err)
	}
	documents := decodeStrimziDocuments[strimziUserSpec](t, out.Bytes())
	if len(documents) != 2 {
		t.Fatalf("expected two documents, got %d", len(documents))
	}
	for i, name := range []string{"alice", "bob"} {
		document := documents[i]
		if document.APIVersion != "kafka.strimzi.io/v1beta2" || document.Kind != "KafkaUser" || document.Metadata.Name != name {
			t.Errorf("unexpected manifest: %+v", document)
		}
		if document.Spec.Authorization.Type != "simple" {
			t.Errorf("unexpected authorization type: %q", document.Spec.Authorization.Type)
		}
	}
	aliceACLs := documents[0].Spec.Authorization.ACLs
	if len(aliceACLs) != 2 {
		t.Fatalf("expected two merged ACLs, got %d", len(aliceACLs))
	}
	wantResource := strimziACLResource{Type: "topic", Name: "orders", PatternType: "literal"}
	if aliceACLs[0].Resource != wantResource || !reflect.DeepEqual(aliceACLs[0].Operations, []string{"Read", "Write"}) {
		t.Errorf("unexpected merged ACL: %+v", aliceACLs[0])
	}
	if aliceACLs[0].Host != nil || aliceACLs[0].Type != "" {
		t.Errorf("wildcard host and allow type must be omitted: %+v", aliceACLs[0])
	}
	if aliceACLs[1].Host == nil || *aliceACLs[1].Host != "192.0.2.1" || aliceACLs[1].Type != "deny" {
		t.Errorf("unexpected deny ACL: %+v", aliceACLs[1])
	}
}

func TestFormatACLStrimziRejectsInvalidNamesBeforeWriting(t *testing.T) {
	tests := []struct {
		name      string
		principal string
	}{
		{"document injection", "User:alice\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: injected"},
		{"newline", "User:alice\nbob"},
		{"carriage return", "User:alice\rbob"},
		{"tab", "User:alice\tbob"},
		{"control character", "User:alice\x00bob"},
		{"empty", "User:"},
		{"uppercase", "User:Alice"},
		{"wildcard", "User:*"},
		{"space", "User:alice bob"},
		{"underscore", "User:alice_bob"},
		{"double quote", "User:alice\""},
		{"single quote", "User:alice'"},
		{"mapping", "User:alice: admin"},
		{"comment", "User:alice # ignored"},
		{"alias", "User:*alice"},
		{"tag", "User:!!str alice"},
		{"flow mapping", "User:{name: alice}"},
		{"sequence", "User:[alice]"},
		{"leading hyphen", "User:-alice"},
		{"trailing hyphen", "User:alice-"},
		{"empty label", "User:alice..bob"},
		{"leading dot", "User:.alice"},
		{"trailing dot", "User:alice."},
		{"invalid label boundary", "User:alice.-bob"},
		{"too long", "User:" + strings.Repeat("a", 254)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := formatACLStrimzi(&out, aclResources("User:valid", tt.principal))
			if err == nil || !strings.Contains(err.Error(), "cannot export principal") {
				t.Fatalf("expected invalid principal error, got %v", err)
			}
			if out.Len() != 0 {
				t.Fatalf("invalid export wrote partial manifests: %s", &out)
			}
		})
	}
}

func TestFormatACLStrimziPreservesValidNamesAsStrings(t *testing.T) {
	for _, name := range []string{"alice", "team.alice-1", "0", "123", "null", "true", "yes", strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			if err := formatACLStrimzi(&out, aclResources("User:"+name)); err != nil {
				t.Fatal(err)
			}
			var document struct {
				Metadata map[string]any `yaml:"metadata"`
			}
			if err := yaml.Unmarshal(out.Bytes(), &document); err != nil {
				t.Fatal(err)
			}
			if got := document.Metadata["name"]; got != name {
				t.Errorf("expected name string %q, got %#v", name, got)
			}
		})
	}
}

func TestFormatACLStrimziEscapesResourceAndHost(t *testing.T) {
	values := []string{
		"*", "", "null", "true", "123", "# comment", "&anchor", "[array]", "{map: value}",
		"quotes ' \" and colon:", "backslash\\tab\t", "control\x00character", "line\rreturn",
		"orders\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: injected",
	}
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			resources := aclResources("User:alice")
			resources[0].ResourceName = value
			resources[0].ACLs[0].Host = value
			var out bytes.Buffer
			if err := formatACLStrimzi(&out, resources); err != nil {
				t.Fatal(err)
			}
			documents := decodeStrimziDocuments[strimziUserSpec](t, out.Bytes())
			if len(documents) != 1 || len(documents[0].Spec.Authorization.ACLs) != 1 {
				t.Fatalf("unexpected injected document or ACL: %+v", documents)
			}
			acl := documents[0].Spec.Authorization.ACLs[0]
			if acl.Resource.Name != value {
				t.Errorf("resource name changed: got %q, want %q", acl.Resource.Name, value)
			}
			if value == "*" {
				if acl.Host != nil {
					t.Errorf("wildcard host must be omitted, got %q", *acl.Host)
				}
			} else if acl.Host == nil || *acl.Host != value {
				t.Errorf("host did not round-trip: %+v", acl)
			}
		})
	}
}

func TestFormatTopicStrimzi(t *testing.T) {
	injection := "value\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: injected"
	topics := []*kafka.TopicDetails{
		{
			Name:              injection,
			Partitions:        3,
			ReplicationFactor: 2,
			Config: map[string]string{
				"retention.ms": "123", "unclean.leader.election.enable": "false",
				"null": "null", "multiline": injection, injection: "data", "control": "\x00",
			},
		},
		{Name: "orders", Partitions: 1, ReplicationFactor: 1},
	}
	var out bytes.Buffer
	if err := formatTopicListStrimzi(&out, topics); err != nil {
		t.Fatal(err)
	}
	documents := decodeStrimziDocuments[strimziTopicSpec](t, out.Bytes())
	if len(documents) != len(topics) {
		t.Fatalf("expected %d documents, got %d", len(topics), len(documents))
	}
	for i, topic := range topics {
		document := documents[i]
		if document.APIVersion != "kafka.strimzi.io/v1beta2" || document.Kind != "KafkaTopic" || document.Metadata.Name != topic.Name {
			t.Errorf("unexpected topic manifest: %+v", document)
		}
		if document.Spec.Partitions != topic.Partitions || document.Spec.Replicas != topic.ReplicationFactor || !reflect.DeepEqual(document.Spec.Config, topic.Config) {
			t.Errorf("topic details did not round-trip: %+v", document.Spec)
		}
	}
	firstOutput := out.String()
	out.Reset()
	if err := formatTopicListStrimzi(&out, topics); err != nil {
		t.Fatal(err)
	}
	if out.String() != firstOutput {
		t.Error("topic output must be deterministic")
	}
	out.Reset()
	if err := formatTopicStrimzi(&out, topics[0]); err != nil {
		t.Fatal(err)
	}
	var document struct {
		Spec struct {
			Config map[string]any `yaml:"config"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	for key, want := range topics[0].Config {
		if got := document.Spec.Config[key]; got != want {
			t.Errorf("config %q must remain a string: got %#v, want %q", key, got, want)
		}
	}
}

type failingManifestWriter struct {
	err error
}

func (w failingManifestWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestStrimziOutputErrors(t *testing.T) {
	topic := &kafka.TopicDetails{Name: "orders", Partitions: 1, ReplicationFactor: 1}
	tests := []struct {
		name   string
		format func(io.Writer) error
	}{
		{"acl", func(w io.Writer) error { return formatACLStrimzi(w, aclResources("User:alice")) }},
		{"topic", func(w io.Writer) error { return formatTopicStrimzi(w, topic) }},
		{"topics", func(w io.Writer) error { return formatTopicListStrimzi(w, []*kafka.TopicDetails{topic}) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := errors.New("output unavailable")
			if err := tt.format(failingManifestWriter{err: want}); !errors.Is(err, want) {
				t.Errorf("expected writer error, got %v", err)
			}
			if err := tt.format(failingManifestWriter{}); !errors.Is(err, io.ErrShortWrite) {
				t.Errorf("expected short write error, got %v", err)
			}
		})
	}
}

func TestStrimziEmptyOutput(t *testing.T) {
	var out bytes.Buffer
	if err := formatACLStrimzi(&out, nil); err != nil {
		t.Fatal(err)
	}
	if err := formatTopicListStrimzi(&out, nil); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatalf("expected no documents, got %s", &out)
	}
}

func TestGetCommandsPropagateErrors(t *testing.T) {
	oldMechanism, oldPrompt := saslMechanism, promptPassword
	saslMechanism, promptPassword = "unsupported-test", false
	t.Cleanup(func() {
		saslMechanism, promptPassword = oldMechanism, oldPrompt
	})
	for _, args := range [][]string{{"acl"}, {"acls"}, {"topic", "orders"}, {"topics"}} {
		t.Run(args[0], func(t *testing.T) {
			args = append(args, "-o", "strimzi")
			output, err := executeCommand(newGetCmd(), args...)
			if err == nil || !strings.Contains(err.Error(), "unsupported SASL mechanism") {
				t.Fatalf("expected command failure before connecting, got %v", err)
			}
			if strings.Contains(output, "apiVersion:") {
				t.Fatalf("failed command emitted a manifest: %s", output)
			}
		})
	}
}
