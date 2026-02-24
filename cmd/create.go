package cmd

import (
	"github.com/spf13/cobra"
)

func newCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create resources",
		Long:  `Create Kafka resources like topics and ACLs.`,
	}

	// Add subcommands for different resource types
	cmd.AddCommand(
		newCreateTopicCmd(),
		newCreateACLCmd(),
	)

	return cmd
}

// Create topic
func newCreateTopicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic [name]",
		Short: "Create a new topic",
		Args:  cobra.ExactArgs(1),
		Run:   runTopicCreate,
	}
	cmd.Flags().IntP("partitions", "p", 1, "Number of partitions")
	cmd.Flags().IntP("replication-factor", "r", 1, "Replication factor")
	return cmd
}

// Create ACL
func newCreateACLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Create a new ACL",
		Run:   runACLCreate,
	}
	cmd.Flags().String("resource-type", "", "Resource type (e.g., TOPIC)")
	cmd.Flags().String("resource-name", "", "Resource name")
	cmd.Flags().String("principal", "", "Principal (e.g., User:alice)")
	cmd.Flags().String("host", "*", "Host")
	cmd.Flags().String("operation", "", "Operation (e.g., READ)")
	cmd.Flags().String("permission", "", "Permission (e.g., ALLOW)")
	_ = cmd.RegisterFlagCompletionFunc("resource-type", completeACLResourceTypes())
	_ = cmd.RegisterFlagCompletionFunc("resource-name", completeACLResourceNames())
	_ = cmd.RegisterFlagCompletionFunc("operation", completeACLOperations())
	_ = cmd.RegisterFlagCompletionFunc("permission", completeACLPermissions())
	return cmd
}
