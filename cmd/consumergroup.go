package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/janfonas/kafka-admin-cli/internal/kafka"
	"github.com/spf13/cobra"
)

func runConsumerGroupList(cmd *cobra.Command, args []string) {
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

	// List consumer groups
	groups, err := client.ListConsumerGroups(ctx)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	// Print consumer groups
	for _, group := range groups {
		fmt.Fprintln(cmd.OutOrStdout(), group)
	}
}

func runConsumerGroupGet(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: group ID is required")
		return
	}

	ctx := context.Background()
	groupID := args[0]

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

	// Get consumer group details
	details, err := client.GetConsumerGroup(ctx, groupID)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	// Print consumer group details
	fmt.Fprintf(cmd.OutOrStdout(), "Group ID: %s\n", groupID)
	fmt.Fprintf(cmd.OutOrStdout(), "State: %s\n", details.State)
	fmt.Fprintln(cmd.OutOrStdout(), "Members:")
	for _, member := range details.Members {
		fmt.Fprintf(cmd.OutOrStdout(), "  Client ID: %s\n", member.ClientID)
		fmt.Fprintf(cmd.OutOrStdout(), "  Client Host: %s\n", member.ClientHost)
		fmt.Fprintln(cmd.OutOrStdout(), "  Assignments:")
		for topic, partitions := range member.Assignments {
			fmt.Fprintf(cmd.OutOrStdout(), "    Topic: %s\n", topic)
			fmt.Fprintf(cmd.OutOrStdout(), "    Partitions: %v\n", partitions)
		}
		fmt.Fprintln(cmd.OutOrStdout())
	}

	fmt.Fprintln(cmd.OutOrStdout(), "Offsets:")
	for topic, partitions := range details.Offsets {
		fmt.Fprintf(cmd.OutOrStdout(), "  Topic: %s\n", topic)
		for partition, offset := range partitions {
			fmt.Fprintf(cmd.OutOrStdout(), "    Partition: %d\n", partition)
			fmt.Fprintf(cmd.OutOrStdout(), "    Current: %d\n", offset.Current)
			fmt.Fprintf(cmd.OutOrStdout(), "    End: %d\n", offset.End)
			fmt.Fprintf(cmd.OutOrStdout(), "    Lag: %d\n", offset.Lag)
		}
	}
}

func runConsumerGroupSetOffsets(cmd *cobra.Command, args []string) {
	if len(args) < 4 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: group ID, topic, partition, and offset are required")
		return
	}

	ctx := context.Background()
	groupID := args[0]
	topic := args[1]

	partition, err := strconv.ParseInt(args[2], 10, 32)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: invalid partition: %v\n", err)
		return
	}

	offset, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: invalid offset: %v\n", err)
		return
	}

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

	// Set consumer group offsets
	err = client.SetConsumerGroupOffsets(ctx, groupID, topic, int32(partition), offset)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "Consumer group offsets set successfully")
}

func runConsumerGroupDelete(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: group ID is required")
		return
	}

	ctx := context.Background()
	groupID := args[0]

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

	// Delete consumer group
	err = client.DeleteConsumerGroup(ctx, groupID)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Consumer group %s deleted successfully\n", groupID)
}
