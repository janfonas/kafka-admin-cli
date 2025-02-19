package cmd

import (
	"context"
	"fmt"

	"github.com/a00262/kafka-admin-cli/internal/kafka"
	"github.com/spf13/cobra"
)

var (
	partition int32
	offset    int64
)

func init() {
	// List consumer groups command
	listConsumerGroupsCmd := &cobra.Command{
		Use:   "list",
		Short: "List all consumer groups",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			groups, err := client.ListConsumerGroups(context.Background())
			if err != nil {
				return fmt.Errorf("failed to list consumer groups: %w", err)
			}

			if len(groups) == 0 {
				fmt.Println("No consumer groups found")
				return nil
			}

			fmt.Println("Consumer Groups:")
			for _, group := range groups {
				fmt.Printf("- %s\n", group)
			}
			return nil
		},
	}

	// Get consumer group command
	getConsumerGroupCmd := &cobra.Command{
		Use:   "get [group-id]",
		Short: "Get details of a consumer group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			groupID := args[0]
			details, err := client.GetConsumerGroup(context.Background(), groupID)
			if err != nil {
				return fmt.Errorf("failed to get consumer group details: %w", err)
			}

			fmt.Printf("Group ID: %s\n", groupID)
			fmt.Printf("State: %s\n", details.State)
			fmt.Println("\nMembers:")
			for _, member := range details.Members {
				fmt.Printf("  Client ID: %s\n", member.ClientID)
				fmt.Printf("  Client Host: %s\n", member.ClientHost)
				fmt.Println("  Assignments:")
				for topic, partitions := range member.Assignments {
					fmt.Printf("    Topic: %s\n", topic)
					fmt.Printf("    Partitions: %v\n", partitions)
				}
				fmt.Println()
			}

			fmt.Println("Offsets:")
			for topic, partitions := range details.Offsets {
				fmt.Printf("  Topic: %s\n", topic)
				for partition, offset := range partitions {
					fmt.Printf("    Partition %d: Current=%d, End=%d, Lag=%d\n",
						partition, offset.Current, offset.End, offset.Lag)
				}
			}
			return nil
		},
	}

	// Set offsets command
	setOffsetsCmd := &cobra.Command{
		Use:   "set-offsets [group-id] [topic]",
		Short: "Set consumer group offsets for a topic partition",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			groupID := args[0]
			topic := args[1]

			err = client.SetConsumerGroupOffsets(context.Background(), groupID, topic, partition, offset)
			if err != nil {
				return fmt.Errorf("failed to set consumer group offsets: %w", err)
			}

			fmt.Printf("Successfully set offset %d for partition %d of topic %s in group %s\n",
				offset, partition, topic, groupID)
			return nil
		},
	}
	setOffsetsCmd.Flags().Int32Var(&partition, "partition", 0, "Partition number")
	setOffsetsCmd.Flags().Int64Var(&offset, "offset", 0, "New offset value")
	setOffsetsCmd.MarkFlagRequired("partition")
	setOffsetsCmd.MarkFlagRequired("offset")

	// Consumer group command
	consumerGroupCmd := &cobra.Command{
		Use:   "consumergroup",
		Short: "Manage Kafka consumer groups",
	}
	consumerGroupCmd.AddCommand(listConsumerGroupsCmd)
	consumerGroupCmd.AddCommand(getConsumerGroupCmd)
	consumerGroupCmd.AddCommand(setOffsetsCmd)

	rootCmd.AddCommand(consumerGroupCmd)
}
