package cmd

import (
	"github.com/spf13/cobra"
)

func newDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete resources",
		Long:  `Delete Kafka resources like topics, ACLs, and consumer groups.`,
	}

	// Add subcommands for different resource types
	cmd.AddCommand(
		newDeleteTopicCmd(),
		newDeleteACLCmd(),
		newDeleteConsumerGroupCmd(),
	)

	return cmd
}

// Delete topic
func newDeleteTopicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic [name]",
		Short: "Delete a topic",
		Args:  cobra.ExactArgs(1),
		Run:   runTopicDelete,
	}
	return cmd
}

// Delete ACL
func newDeleteACLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Delete an ACL",
		Run:   runACLDelete,
	}
	cmd.Flags().String("resource-type", "", "Resource type (e.g., TOPIC)")
	cmd.Flags().String("resource-name", "", "Resource name")
	cmd.Flags().String("principal", "", "Principal (e.g., User:alice)")
	cmd.Flags().String("host", "*", "Host")
	cmd.Flags().String("operation", "", "Operation (e.g., READ)")
	cmd.Flags().String("permission", "", "Permission (e.g., ALLOW)")
	return cmd
}

// Delete consumer group
func newDeleteConsumerGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "consumergroup [group-id]",
		Aliases: []string{"cg"},
		Short:   "Delete a consumer group",
		Args:    cobra.ExactArgs(1),
		Run:     runConsumerGroupDelete,
	}
	return cmd
}
