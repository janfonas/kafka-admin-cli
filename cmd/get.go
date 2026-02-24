package cmd

import (
	"github.com/spf13/cobra"
)

func newGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get/list resources",
		Long:  `Get or list Kafka resources like topics, ACLs, and consumer groups.`,
	}

	// Add subcommands for different resource types
	cmd.AddCommand(
		newGetTopicsCmd(),
		newGetTopicCmd(),
		newGetACLsCmd(),
		newGetACLCmd(),
		newGetConsumerGroupsCmd(),
		newGetConsumerGroupCmd(),
	)

	return cmd
}

// Get all topics
func newGetTopicsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "List all Kafka topics",
		Run:   runTopicList,
	}
	return cmd
}

// Get specific topic
func newGetTopicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "topic [name]",
		Short:             "Get details of a specific topic",
		Args:              cobra.ExactArgs(1),
		Run:               runTopicGet,
		ValidArgsFunction: completeTopicNames,
	}
	return cmd
}

// Get all ACLs
func newGetACLsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acls",
		Short: "List all Kafka ACLs",
		Run:   runACLList,
	}
	cmd.Flags().StringP("output", "o", "table", "Output format (table, strimzi)")
	_ = cmd.RegisterFlagCompletionFunc("output", completeOutputFormats())
	return cmd
}

// Get specific ACL
func newGetACLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Get ACL details",
		Run:   runACLGet,
	}
	cmd.Flags().String("resource-type", "", "Resource type (e.g., TOPIC)")
	cmd.Flags().String("resource-name", "", "Resource name")
	cmd.Flags().String("principal", "", "Principal (e.g., User:alice)")
	cmd.Flags().StringP("output", "o", "table", "Output format (table, strimzi)")
	_ = cmd.RegisterFlagCompletionFunc("resource-type", completeACLResourceTypes())
	_ = cmd.RegisterFlagCompletionFunc("resource-name", completeACLResourceNames())
	_ = cmd.RegisterFlagCompletionFunc("output", completeOutputFormats())
	return cmd
}

// Get all consumer groups
func newGetConsumerGroupsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "consumergroups",
		Aliases: []string{"cg"},
		Short:   "List all consumer groups",
		Run:     runConsumerGroupList,
	}
	return cmd
}

// Get specific consumer group
func newGetConsumerGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "consumergroup [group-id]",
		Aliases:           []string{"cg"},
		Short:             "Get consumer group details",
		Args:              cobra.ExactArgs(1),
		Run:               runConsumerGroupGet,
		ValidArgsFunction: completeConsumerGroupIDs,
	}
	return cmd
}
