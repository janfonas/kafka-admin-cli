package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/janfonas/kafka-admin-cli/internal/kafka"
)

func TestStrimziTopicResourceName(t *testing.T) {
	tests := []struct {
		topic string
		want  string
	}{
		{"orders", "orders"},
		{"foo.bar", "foo.bar"},
		{"foo--bar", "foo--bar"},
		{"Orders_V2", "orders-v2---a6c5c2023b9e362e528406cf23ecc025d670d395"},
		{"__consumer_offsets", "consumer-offsets---84e7a678d08f4bd226872e5cdd4eb527fadc1c6a"},
		{"__foo", "foo---117282ae03235561f215ee101800b2d5b3609f7"},
		{"Foo", "foo---201a6b3053cc1422d2c3670b62616221d2290929"},
		{"FOO", "foo---feab40e1fca77c7360ccca1481bb8ba5f919ce3a"},
		{"foo__bar", "foo-bar---423126006221ca9092992ce14121ee0a3eaed346"},
		{"foo_bar", "foo-bar---5d5f20e74771b852025c3edb66c9462eeb913d03"},
		{"foo.-bar", "foo.bar---4f4325245861ad879d835e7fca95145c2f8c5eb8"},
		{"foo-.bar", "foo-bar---6ba1b74e7c34254171fabf7cdfbba9936356e940"},
		{"foo..", "foo---ab2e4e62f3b8c288495eb2616ee2cd678f83a63f"},
		{"-", "3bc15c8aae3e4124dd409035f32ea2fd6835efc9"},
		{"...", "6eae3a5b062c6d0d79f070c26e6d62486b40cb46"},
		{strings.Repeat("a", 249), strings.Repeat("a", 249)},
	}
	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := strimziTopicResourceName(tt.topic)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
			if !isValidKubernetesName(got) {
				t.Errorf("invalid Kubernetes name: %q", got)
			}
		})
	}
}

func TestStrimziTopicResourceNameTruncation(t *testing.T) {
	for _, topic := range []string{
		strings.Repeat("A", 249),
		strings.Repeat("a", 209) + "." + strings.Repeat("A", 39),
		strings.Repeat("a", 209) + "-" + strings.Repeat("A", 39),
	} {
		t.Run(topic, func(t *testing.T) {
			got := strimziTopicResourceName(topic)
			if len(got) > maxKubernetesNameLength || !isValidKubernetesName(got) {
				t.Fatalf("invalid truncated resource name (%d bytes): %q", len(got), got)
			}
			if !strings.Contains(got, "---") {
				t.Errorf("truncated resource name lacks hash separator: %q", got)
			}
			if again := strimziTopicResourceName(topic); got != again {
				t.Errorf("mapping is not deterministic: %q != %q", got, again)
			}
		})
	}
}

func TestFormatTopicStrimziPreservesMappedNames(t *testing.T) {
	topics := []*kafka.TopicDetails{
		{Name: "Foo", Partitions: 1, ReplicationFactor: 1},
		{Name: "FOO", Partitions: 2, ReplicationFactor: 1},
		{Name: "foo", Partitions: 3, ReplicationFactor: 1},
	}
	var out bytes.Buffer
	if err := formatTopicListStrimzi(&out, topics); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(out.String(), "\n---\n"); got != len(topics)-1 {
		t.Errorf("expected %d document separators, got %d", len(topics)-1, got)
	}
	documents := decodeStrimziDocuments[strimziTopicSpec](t, out.Bytes())
	if len(documents) != len(topics) {
		t.Fatalf("expected %d topics, got %d", len(topics), len(documents))
	}
	seen := make(map[string]bool)
	for i, document := range documents {
		if seen[document.Metadata.Name] {
			t.Errorf("mapped names collide: %q", document.Metadata.Name)
		}
		seen[document.Metadata.Name] = true
		if document.Metadata.Name != strimziTopicResourceName(topics[i].Name) {
			t.Errorf("unexpected metadata name: %q", document.Metadata.Name)
		}
		if i == 2 {
			if document.Spec.TopicName != "" {
				t.Errorf("valid Kubernetes name must not need spec.topicName: %+v", document)
			}
		} else if document.Spec.TopicName != topics[i].Name {
			t.Errorf("Kafka topic identity changed: got %q, want %q", document.Spec.TopicName, topics[i].Name)
		}
	}
}

func TestFormatTopicStrimziRejectsNameCollisions(t *testing.T) {
	names := []string{"Foo", "foo---201a6b3053cc1422d2c3670b62616221d2290929"}
	for _, order := range [][]string{names, {names[1], names[0]}} {
		t.Run(order[0], func(t *testing.T) {
			topics := []*kafka.TopicDetails{{Name: order[0]}, {Name: order[1]}}
			var out bytes.Buffer
			err := formatTopicListStrimzi(&out, topics)
			if err == nil || !strings.Contains(err.Error(), "both map to KafkaTopic") {
				t.Fatalf("expected collision error, got %v", err)
			}
			if out.Len() != 0 {
				t.Fatalf("colliding topics wrote partial manifests: %s", &out)
			}
		})
	}
}

func TestFormatTopicStrimziRejectsEmptyName(t *testing.T) {
	var out bytes.Buffer
	err := formatTopicListStrimzi(&out, []*kafka.TopicDetails{{Name: "valid"}, {Name: ""}})
	if err == nil || !strings.Contains(err.Error(), "empty name") {
		t.Fatalf("expected empty name error, got %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("invalid topic wrote partial manifests: %s", &out)
	}
}
