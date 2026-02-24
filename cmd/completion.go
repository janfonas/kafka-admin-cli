package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/janfonas/kafka-admin-cli/internal/credentials"
	"github.com/janfonas/kafka-admin-cli/internal/kafka"
	"github.com/spf13/cobra"
)

const completionTimeout = 5 * time.Second

// newCompletionClient creates a Kafka client for completion by loading
// credentials from the active profile. Returns nil if credentials are
// unavailable (completion silently returns no suggestions).
func newCompletionClient() *kafka.Client {
	// Try to load credentials from the active profile
	profileName := profile
	if profileName == "" || profileName == "default" {
		if active := credentials.GetActiveProfile(); active != "" {
			profileName = active
		} else {
			profileName = "default"
		}
	}

	prof, err := credentials.Load(profileName)
	if err != nil {
		return nil
	}

	b := brokers
	u := username
	p := password
	s := saslMechanism
	c := caCertPath
	i := insecure

	if b == "" {
		b = prof.Brokers
	}
	if u == "" {
		u = prof.Username
	}
	if p == "" {
		p = prof.Password
	}
	if s == "SCRAM-SHA-512" && prof.SASLMechanism != "" {
		s = prof.SASLMechanism
	}
	if c == "" {
		c = prof.CACertPath
	}
	if !i {
		i = prof.Insecure
	}

	if b == "" || u == "" || p == "" {
		return nil
	}

	client, err := kafka.NewClient(strings.Split(b, ","), u, p, c, s, i)
	if err != nil {
		return nil
	}
	return client
}

// completeTopicNames provides dynamic completion of Kafka topic names.
func completeTopicNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	client := newCompletionClient()
	if client == nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
	defer cancel()

	topics, err := client.ListTopics(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	var matches []string
	for _, t := range topics {
		if strings.HasPrefix(t, toComplete) {
			matches = append(matches, t)
		}
	}
	return matches, cobra.ShellCompDirectiveNoFileComp
}

// completeConsumerGroupIDs provides dynamic completion of Kafka consumer group IDs.
func completeConsumerGroupIDs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	client := newCompletionClient()
	if client == nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
	defer cancel()

	groups, err := client.ListConsumerGroups(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	var matches []string
	for _, g := range groups {
		if strings.HasPrefix(g, toComplete) {
			matches = append(matches, g)
		}
	}
	return matches, cobra.ShellCompDirectiveNoFileComp
}

// completeSetOffsetsArgs provides positional completion for the set-offsets
// consumergroup command: [group-id] [topic] [partition] [offset].
func completeSetOffsetsArgs(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	switch len(args) {
	case 0:
		// Complete consumer group ID
		return completeConsumerGroupIDs(cmd, nil, toComplete)
	case 1:
		// Complete topic name
		return completeTopicNames(cmd, nil, toComplete)
	case 2:
		// Complete partition number — fetch partition count for the topic
		client := newCompletionClient()
		if client == nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
		defer cancel()

		details, err := client.GetTopic(ctx, args[1])
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		partitions := make([]string, details.Partitions)
		for i := int32(0); i < details.Partitions; i++ {
			partitions[i] = fmt.Sprintf("%d", i)
		}
		return partitions, cobra.ShellCompDirectiveNoFileComp
	case 3:
		// Complete offset — offer common presets
		return []string{
			"0\treset to beginning",
			"-1\treset to end (latest)",
			"-2\treset to earliest",
		}, cobra.ShellCompDirectiveNoFileComp
	default:
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

// completeProfileNames provides dynamic completion of stored credential profile names.
func completeProfileNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	profiles, err := credentials.List()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	var matches []string
	for _, p := range profiles {
		if strings.HasPrefix(p.Name, toComplete) {
			desc := p.Brokers
			if p.IsActive {
				desc += " (active)"
			}
			matches = append(matches, p.Name+"\t"+desc)
		}
	}
	return matches, cobra.ShellCompDirectiveNoFileComp
}

// registerProfileFlagCompletion registers completion for --profile flags on a command.
func registerProfileFlagCompletion(cmd *cobra.Command) {
	_ = cmd.RegisterFlagCompletionFunc("profile", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		profiles, err := credentials.List()
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		var matches []string
		for _, p := range profiles {
			if strings.HasPrefix(p.Name, toComplete) {
				matches = append(matches, p.Name)
			}
		}
		return matches, cobra.ShellCompDirectiveNoFileComp
	})
}

// registerSASLMechanismCompletion registers completion for the --sasl-mechanism flag.
func registerSASLMechanismCompletion(cmd *cobra.Command) {
	_ = cmd.RegisterFlagCompletionFunc("sasl-mechanism", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"SCRAM-SHA-512", "PLAIN"}, cobra.ShellCompDirectiveNoFileComp
	})
}

// completeACLResourceTypes provides completion for ACL resource type values.
func completeACLResourceTypes() func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{
			"1\tAny",
			"2\tTopic",
			"3\tGroup",
			"4\tCluster",
			"5\tTransactionalID",
			"6\tDelegationToken",
		}, cobra.ShellCompDirectiveNoFileComp
	}
}

// completeACLOperations provides completion for ACL operation values.
func completeACLOperations() func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{
			"1\tAny",
			"2\tAll",
			"3\tRead",
			"4\tWrite",
			"5\tCreate",
			"6\tDelete",
			"7\tAlter",
			"8\tDescribe",
			"9\tClusterAction",
			"10\tDescribeConfigs",
			"11\tAlterConfigs",
			"12\tIdempotentWrite",
		}, cobra.ShellCompDirectiveNoFileComp
	}
}

// completeACLPermissions provides completion for ACL permission type values.
func completeACLPermissions() func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{
			"1\tAny",
			"2\tDeny",
			"3\tAllow",
		}, cobra.ShellCompDirectiveNoFileComp
	}
}

// completeACLResourceNames provides dynamic completion for ACL --resource-name
// based on the current --resource-type flag value.
func completeACLResourceNames() func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		resType, _ := cmd.Flags().GetString("resource-type")
		resTypeInt, err := strconv.Atoi(resType)
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}

		client := newCompletionClient()
		if client == nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
		defer cancel()

		switch resTypeInt {
		case 2: // Topic
			topics, err := client.ListTopics(ctx)
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			var matches []string
			for _, t := range topics {
				if strings.HasPrefix(t, toComplete) {
					matches = append(matches, t)
				}
			}
			return matches, cobra.ShellCompDirectiveNoFileComp
		case 3: // Group
			groups, err := client.ListConsumerGroups(ctx)
			if err != nil {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
			var matches []string
			for _, g := range groups {
				if strings.HasPrefix(g, toComplete) {
					matches = append(matches, g)
				}
			}
			return matches, cobra.ShellCompDirectiveNoFileComp
		default:
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
	}
}
