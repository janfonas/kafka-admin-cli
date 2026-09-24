package cmd

import (
	"crypto/sha1"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/janfonas/kafka-admin-cli/internal/kafka"
)

type strimziTopicSpec struct {
	TopicName  string            `yaml:"topicName,omitempty"`
	Partitions int32             `yaml:"partitions"`
	Replicas   int16             `yaml:"replicas"`
	Config     map[string]string `yaml:"config,omitempty"`
}

// formatTopicTable prints topic details in the default human-readable format.
func formatTopicTable(w io.Writer, details *kafka.TopicDetails) {
	fmt.Fprintf(w, "Name: %s\n", details.Name)
	fmt.Fprintf(w, "Partitions: %d\n", details.Partitions)
	fmt.Fprintf(w, "Replication Factor: %d\n", details.ReplicationFactor)
	if len(details.Config) > 0 {
		fmt.Fprintln(w, "Config:")
		for k, v := range details.Config {
			fmt.Fprintf(w, "  %s: %s\n", k, v)
		}
	}
}

// formatTopicStrimzi renders a single topic as a Strimzi KafkaTopic CR YAML manifest.
func formatTopicStrimzi(w io.Writer, details *kafka.TopicDetails) error {
	return formatTopicListStrimzi(w, []*kafka.TopicDetails{details})
}

// formatTopicListStrimzi renders multiple topics as Strimzi KafkaTopic CR YAML manifests.
func formatTopicListStrimzi(w io.Writer, topics []*kafka.TopicDetails) error {
	manifests := make([]strimziManifest[strimziTopicSpec], 0, len(topics))
	topicByName := make(map[string]string)
	for _, details := range topics {
		if details.Name == "" {
			return fmt.Errorf("cannot export a topic with an empty name as a KafkaTopic")
		}
		resourceName := strimziTopicResourceName(details.Name)
		if previous, exists := topicByName[resourceName]; exists && previous != details.Name {
			return fmt.Errorf("cannot export topics %q and %q: both map to KafkaTopic %q; refusing to overwrite either topic", previous, details.Name, resourceName)
		}
		topicByName[resourceName] = details.Name
		spec := strimziTopicSpec{
			Partitions: details.Partitions,
			Replicas:   details.ReplicationFactor,
			Config:     details.Config,
		}
		if resourceName != details.Name {
			spec.TopicName = details.Name
		}
		manifests = append(manifests, strimziManifest[strimziTopicSpec]{
			APIVersion: "kafka.strimzi.io/v1beta2",
			Kind:       "KafkaTopic",
			Metadata:   strimziMetadata{Name: resourceName},
			Spec:       spec,
		})
	}
	return writeStrimziManifests(w, manifests)
}

// Matches the legacy Strimzi TopicName.asKubeName mapping:
// https://github.com/strimzi/strimzi-kafka-operator/blob/0.40.0/topic-operator/src/main/java/io/strimzi/operator/topic/TopicName.java
func strimziTopicResourceName(topicName string) string {
	if isValidKubernetesName(topicName) {
		return topicName
	}

	var prefix []byte
	for i := 0; i < len(topicName); i++ {
		ch := topicName[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9':
			prefix = append(prefix, ch)
		case ch >= 'A' && ch <= 'Z':
			prefix = append(prefix, ch+'a'-'A')
		case ch == '-', ch == '.', ch == '_':
			if len(prefix) > 0 && prefix[len(prefix)-1] != '-' && prefix[len(prefix)-1] != '.' {
				if ch == '_' {
					ch = '-'
				}
				prefix = append(prefix, ch)
			}
		}
	}
	name := strings.TrimRight(string(prefix), ".-")
	const separator = "---"
	maxPrefixLength := maxKubernetesNameLength - len(separator) - sha1.Size*2
	if len(name) > maxPrefixLength {
		// A dot exposed by truncation would make the hash start an invalid DNS label.
		name = strings.TrimRight(name[:maxPrefixLength], ".")
	}

	// SHA-1 is only a compatibility suffix, not a security checksum. Strimzi omits leading hex zeros.
	digest := sha1.Sum([]byte(topicName))
	hash := new(big.Int).SetBytes(digest[:]).Text(16)
	if name == "" {
		return hash
	}
	return name + separator + hash
}
