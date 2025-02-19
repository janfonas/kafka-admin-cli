package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/janfonas/kafka-admin-cli/internal/kafka"
	"github.com/spf13/cobra"
)

func runTopicList(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Get password if not provided
	if promptPassword {
		var err error
		password, err = getPassword()
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
	}

	// Create Kafka client
	client, err := kafka.NewClient(strings.Split(brokers, ","), username, password, caCertPath, saslMechanism, insecure)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}
	defer client.Close()

	// List topics
	topics, err := client.ListTopics(ctx)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	// Print topics
	for _, topic := range topics {
		fmt.Fprintln(cmd.OutOrStdout(), topic)
	}
}

func runTopicCreate(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: topic name is required")
		return
	}

	ctx := context.Background()
	topic := args[0]

	// Get flags
	partitions, _ := cmd.Flags().GetInt("partitions")
	replicationFactor, _ := cmd.Flags().GetInt("replication-factor")

	// Get password if not provided
	if promptPassword {
		var err error
		password, err = getPassword()
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
	}

	// Create Kafka client
	client, err := kafka.NewClient(strings.Split(brokers, ","), username, password, caCertPath, saslMechanism, insecure)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}
	defer client.Close()

	// Create topic
	err = client.CreateTopic(ctx, topic, partitions, replicationFactor)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Topic %s created successfully\n", topic)
}

func runTopicDelete(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: topic name is required")
		return
	}

	ctx := context.Background()
	topic := args[0]

	// Get password if not provided
	if promptPassword {
		var err error
		password, err = getPassword()
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
	}

	// Create Kafka client
	client, err := kafka.NewClient(strings.Split(brokers, ","), username, password, caCertPath, saslMechanism, insecure)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}
	defer client.Close()

	// Delete topic
	err = client.DeleteTopic(ctx, topic)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Topic %s deleted successfully\n", topic)
}

func runTopicGet(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: topic name is required")
		return
	}

	ctx := context.Background()
	topic := args[0]

	// Get password if not provided
	if promptPassword {
		var err error
		password, err = getPassword()
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
	}

	// Create Kafka client
	client, err := kafka.NewClient(strings.Split(brokers, ","), username, password, caCertPath, saslMechanism, insecure)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}
	defer client.Close()

	// Get topic details
	details, err := client.GetTopic(ctx, topic)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	// Print topic details
	fmt.Fprintf(cmd.OutOrStdout(), "Name: %s\n", details.Name)
	fmt.Fprintf(cmd.OutOrStdout(), "Partitions: %d\n", details.Partitions)
	fmt.Fprintf(cmd.OutOrStdout(), "Replication Factor: %d\n", details.ReplicationFactor)
	if len(details.Config) > 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "Config:")
		for k, v := range details.Config {
			fmt.Fprintf(cmd.OutOrStdout(), "  %s: %s\n", k, v)
		}
	}
}
