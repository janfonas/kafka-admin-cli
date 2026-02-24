package cmd

import (
	"fmt"
	"io"
	"sort"

	"github.com/janfonas/kafka-admin-cli/internal/kafka"
)

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
func formatTopicStrimzi(w io.Writer, details *kafka.TopicDetails) {
	fmt.Fprintln(w, "apiVersion: kafka.strimzi.io/v1beta2")
	fmt.Fprintln(w, "kind: KafkaTopic")
	fmt.Fprintln(w, "metadata:")
	fmt.Fprintf(w, "  name: %s\n", yamlQuoteIfNeeded(details.Name))
	fmt.Fprintln(w, "spec:")
	fmt.Fprintf(w, "  partitions: %d\n", details.Partitions)
	fmt.Fprintf(w, "  replicas: %d\n", details.ReplicationFactor)
	if len(details.Config) > 0 {
		fmt.Fprintln(w, "  config:")
		// Sort keys for deterministic output
		keys := make([]string, 0, len(details.Config))
		for k := range details.Config {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "    %s: %s\n", k, yamlQuoteIfNeeded(details.Config[k]))
		}
	}
}

// formatTopicListStrimzi renders multiple topics as Strimzi KafkaTopic CR YAML manifests.
func formatTopicListStrimzi(w io.Writer, topics []*kafka.TopicDetails) {
	for i, details := range topics {
		if i > 0 {
			fmt.Fprintln(w, "---")
		}
		formatTopicStrimzi(w, details)
	}
}
