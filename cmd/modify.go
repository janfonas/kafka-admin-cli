package cmd

import (
	"github.com/spf13/cobra"
)

func newModifyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "modify",
		Short: "Modify resources",
		Long:  `Modify Kafka resources like topics and ACLs.`,
	}

	// Add subcommands for different resource types
	cmd.AddCommand(
		newModifyTopicCmd(),
		newModifyACLCmd(),
	)

	return cmd
}

// Modify topic
func newModifyTopicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic [name]",
		Short: "Modify topic configuration",
		Args:  cobra.ExactArgs(1),
		Run:   runTopicModify,
	}
	cmd.Flags().StringSliceP("config", "c", nil, "Topic configuration in format key=value (can be specified multiple times)")
	return cmd
}

// Modify ACL
func newModifyACLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Modify an ACL",
		Run:   runACLModify,
	}
	cmd.Flags().String("resource-type", "", "Resource type (e.g., TOPIC)")
	cmd.Flags().String("resource-name", "", "Resource name")
	cmd.Flags().String("principal", "", "Principal (e.g., User:alice)")
	cmd.Flags().String("host", "*", "Host")
	cmd.Flags().String("operation", "", "Operation (e.g., READ)")
	cmd.Flags().String("permission", "", "Current permission (e.g., ALLOW)")
	cmd.Flags().String("new-permission", "", "New permission (e.g., DENY)")
	return cmd
}
