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
	outputFormat, _ := cmd.Flags().GetString("output")

	// Get password if not provided
	if promptPassword {
		var err error
		password, err = getPassword()
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
	}

	// Create Kafka client (suppress status messages for structured output)
	var clientOpts []kafka.ClientOption
	if outputFormat != outputTable {
		clientOpts = append(clientOpts, kafka.WithQuiet())
	}
	client, err := kafka.NewClient(strings.Split(brokers, ","), username, password, caCertPath, saslMechanism, insecure, clientOpts...)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}
	defer client.Close()

	switch outputFormat {
	case outputStrimzi:
		// For strimzi output, fetch full details for each topic
		names, err := client.ListTopics(ctx)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
		var topics []*kafka.TopicDetails
		for _, name := range names {
			details, err := client.GetTopic(ctx, name)
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Warning: skipping topic %s: %v\n", name, err)
				continue
			}
			topics = append(topics, details)
		}
		formatTopicListStrimzi(cmd.OutOrStdout(), topics)
	default:
		topics, err := client.ListTopics(ctx)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
		for _, topic := range topics {
			fmt.Fprintln(cmd.OutOrStdout(), topic)
		}
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

func runTopicModify(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: topic name is required")
		return
	}

	ctx := context.Background()
	topic := args[0]

	// Get flags
	configStr, _ := cmd.Flags().GetStringSlice("config")
	config := make(map[string]string)
	for _, c := range configStr {
		parts := strings.SplitN(c, "=", 2)
		if len(parts) != 2 {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: invalid config format %q, expected key=value\n", c)
			return
		}
		config[parts[0]] = parts[1]
	}

	if len(config) == 0 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: at least one config parameter is required")
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

	// Modify topic
	err = client.ModifyTopic(ctx, topic, config)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Topic %s modified successfully\n", topic)
}

func runTopicGet(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(cmd.ErrOrStderr(), "Error: topic name is required")
		return
	}

	ctx := context.Background()
	topic := args[0]
	outputFormat, _ := cmd.Flags().GetString("output")

	// Get password if not provided
	if promptPassword {
		var err error
		password, err = getPassword()
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
	}

	// Create Kafka client (suppress status messages for structured output)
	var clientOpts []kafka.ClientOption
	if outputFormat != outputTable {
		clientOpts = append(clientOpts, kafka.WithQuiet())
	}
	client, err := kafka.NewClient(strings.Split(brokers, ","), username, password, caCertPath, saslMechanism, insecure, clientOpts...)
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

	switch outputFormat {
	case outputStrimzi:
		formatTopicStrimzi(cmd.OutOrStdout(), details)
	default:
		formatTopicTable(cmd.OutOrStdout(), details)
	}
}
