package cmd

import (
	"context"
	"fmt"

	"github.com/a00262/kafka-admin-cli/internal/kafka"

	"github.com/spf13/cobra"
)

var (
	partitions        int
	replicationFactor int
)

func init() {
	// Create topic command
	createTopicCmd := &cobra.Command{
		Use:   "create [topic]",
		Short: "Create a new topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			topic := args[0]
			err = client.CreateTopic(context.Background(), topic, partitions, replicationFactor)
			if err != nil {
				return fmt.Errorf("failed to create topic: %w", err)
			}

			fmt.Printf("Topic '%s' created successfully\n", topic)
			return nil
		},
	}
	createTopicCmd.Flags().IntVarP(&partitions, "partitions", "p", 1, "Number of partitions")
	createTopicCmd.Flags().IntVarP(&replicationFactor, "replication-factor", "r", 1, "Replication factor")

	// Delete topic command
	deleteTopicCmd := &cobra.Command{
		Use:   "delete [topic]",
		Short: "Delete a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			topic := args[0]
			err = client.DeleteTopic(context.Background(), topic)
			if err != nil {
				return fmt.Errorf("failed to delete topic: %w", err)
			}

			fmt.Printf("Topic '%s' deleted successfully\n", topic)
			return nil
		},
	}

	// List topics command
	listTopicsCmd := &cobra.Command{
		Use:   "list",
		Short: "List all topics",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			topics, err := client.ListTopics(context.Background())
			if err != nil {
				return fmt.Errorf("failed to list topics: %w", err)
			}

			if len(topics) == 0 {
				fmt.Println("No topics found")
				return nil
			}

			fmt.Println("Topics:")
			for _, topic := range topics {
				fmt.Printf("- %s\n", topic)
			}
			return nil
		},
	}

	// Get topic command
	getTopicCmd := &cobra.Command{
		Use:   "get [topic]",
		Short: "Get details of a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			topic := args[0]
			details, err := client.GetTopic(context.Background(), topic)
			if err != nil {
				return fmt.Errorf("failed to get topic details: %w", err)
			}

			fmt.Printf("Topic: %s\n", details.Name)
			fmt.Printf("Partitions: %d\n", details.Partitions)
			fmt.Printf("Replication Factor: %d\n", details.ReplicationFactor)
			if len(details.Config) > 0 {
				fmt.Println("Custom Configurations:")
				for key, value := range details.Config {
					fmt.Printf("  %s: %s\n", key, value)
				}
			}
			return nil
		},
	}

	// Topic command
	topicCmd := &cobra.Command{
		Use:   "topic",
		Short: "Manage Kafka topics",
	}
	topicCmd.AddCommand(createTopicCmd)
	topicCmd.AddCommand(deleteTopicCmd)
	topicCmd.AddCommand(listTopicsCmd)
	topicCmd.AddCommand(getTopicCmd)

	rootCmd.AddCommand(topicCmd)
}
