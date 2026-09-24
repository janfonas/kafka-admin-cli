package cmd

import (
	"fmt"
	"io"

	"github.com/janfonas/kafka-admin-cli/internal/kafka"
)

type strimziTopicSpec struct {
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
	for _, details := range topics {
		manifests = append(manifests, strimziManifest[strimziTopicSpec]{
			APIVersion: "kafka.strimzi.io/v1beta2",
			Kind:       "KafkaTopic",
			Metadata:   strimziMetadata{Name: details.Name},
			Spec: strimziTopicSpec{
				Partitions: details.Partitions,
				Replicas:   details.ReplicationFactor,
				Config:     details.Config,
			},
		})
	}
	return writeStrimziManifests(w, manifests)
}
