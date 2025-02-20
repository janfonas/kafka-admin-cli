package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	rootCmd        *cobra.Command
	brokers        string
	username       string
	password       string
	saslMechanism  string
	caCertPath     string
	insecure       bool
	promptPassword bool
)

func init() {
	rootCmd = NewRootCmd()
	initCommands()
}

func initCommands() {
	rootCmd.AddCommand(
		newVersionCmd(),
		newTopicCmd(),
		newACLCmd(),
		newConsumerGroupCmd(),
	)
}

// GetRootCmd returns the root command for use by other packages
func GetRootCmd() *cobra.Command {
	return rootCmd
}

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kac",
		Short: "Kafka Admin CLI",
		Long: `A command-line interface for Apache Kafka administration.
Provides tools for managing topics, ACLs, and consumer groups.`,
		SilenceUsage:     true,
		SilenceErrors:    true,
		TraverseChildren: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return fmt.Errorf("unknown command %q", args[0])
			}
			return cmd.Help()
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if promptPassword {
				var err error
				password, err = getPassword()
				if err != nil {
					return fmt.Errorf("failed to read password: %w", err)
				}
			}
			return nil
		},
	}

	// Add persistent flags
	cmd.PersistentFlags().StringVarP(&brokers, "brokers", "b", "", "Kafka broker list (comma-separated)")
	cmd.PersistentFlags().StringVarP(&username, "username", "u", "", "SASL username")
	cmd.PersistentFlags().StringVarP(&password, "password", "w", "", "SASL password")
	cmd.PersistentFlags().BoolVarP(&promptPassword, "prompt-password", "P", false, "Prompt for password")
	cmd.PersistentFlags().StringVar(&saslMechanism, "sasl-mechanism", "SCRAM-SHA-512", "SASL mechanism (SCRAM-SHA-512 or PLAIN)")
	cmd.PersistentFlags().StringVar(&caCertPath, "ca-cert", "", "CA certificate file path")
	cmd.PersistentFlags().BoolVar(&insecure, "insecure", false, "Skip TLS certificate verification")

	return cmd
}

func getPassword() (string, error) {
	if password != "" {
		return password, nil
	}

	fmt.Fprint(os.Stderr, "Password: ")
	password, err := readPassword()
	if err != nil {
		return "", err
	}
	return password, nil
}

func readPassword() (string, error) {
	// For now, just read from stdin
	var password string
	_, err := fmt.Scanln(&password)
	return password, err
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newTopicCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topic",
		Short: "Manage Kafka topics",
		Long:  `Create, delete, list, and describe Kafka topics.`,
	}

	// Create command with flags
	createCmd := &cobra.Command{
		Use:   "create [topic]",
		Short: "Create a topic",
		Run:   runTopicCreate,
	}
	createCmd.Flags().IntP("partitions", "p", 1, "Number of partitions")
	createCmd.Flags().IntP("replication-factor", "r", 1, "Replication factor")

	// Modify command with flags
	modifyCmd := &cobra.Command{
		Use:   "modify [topic]",
		Short: "Modify topic configuration",
		Run:   runTopicModify,
	}
	modifyCmd.Flags().StringSliceP("config", "c", nil, "Topic configuration in format key=value (can be specified multiple times)")

	// Add topic subcommands
	cmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List topics",
			Run:   runTopicList,
		},
		createCmd,
		modifyCmd,
		&cobra.Command{
			Use:   "delete [topic]",
			Short: "Delete a topic",
			Run:   runTopicDelete,
		},
		&cobra.Command{
			Use:   "get [topic]",
			Short: "Get topic details",
			Run:   runTopicGet,
		},
	)

	return cmd
}

func newACLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Manage Kafka ACLs",
		Long:  `Create, delete, list, and describe Kafka ACLs.`,
	}

	// Create command with flags
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create an ACL",
		Run:   runACLCreate,
	}
	createCmd.Flags().String("resource-type", "", "Resource type (e.g., TOPIC)")
	createCmd.Flags().String("resource-name", "", "Resource name")
	createCmd.Flags().String("principal", "", "Principal (e.g., User:alice)")
	createCmd.Flags().String("host", "*", "Host")
	createCmd.Flags().String("operation", "", "Operation (e.g., READ)")
	createCmd.Flags().String("permission", "", "Permission (e.g., ALLOW)")

	// Delete command with flags
	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete an ACL",
		Run:   runACLDelete,
	}
	deleteCmd.Flags().String("resource-type", "", "Resource type (e.g., TOPIC)")
	deleteCmd.Flags().String("resource-name", "", "Resource name")
	deleteCmd.Flags().String("principal", "", "Principal (e.g., User:alice)")
	deleteCmd.Flags().String("host", "*", "Host")
	deleteCmd.Flags().String("operation", "", "Operation (e.g., READ)")
	deleteCmd.Flags().String("permission", "", "Permission (e.g., ALLOW)")

	// Get command with flags
	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Get ACL details",
		Run:   runACLGet,
	}
	getCmd.Flags().String("resource-type", "", "Resource type (e.g., TOPIC)")
	getCmd.Flags().String("resource-name", "", "Resource name")
	getCmd.Flags().String("principal", "", "Principal (e.g., User:alice)")

	// Modify command with flags
	modifyCmd := &cobra.Command{
		Use:   "modify",
		Short: "Modify an ACL",
		Run:   runACLModify,
	}
	modifyCmd.Flags().String("resource-type", "", "Resource type (e.g., TOPIC)")
	modifyCmd.Flags().String("resource-name", "", "Resource name")
	modifyCmd.Flags().String("principal", "", "Principal (e.g., User:alice)")
	modifyCmd.Flags().String("host", "*", "Host")
	modifyCmd.Flags().String("operation", "", "Operation (e.g., READ)")
	modifyCmd.Flags().String("permission", "", "Current permission (e.g., ALLOW)")
	modifyCmd.Flags().String("new-permission", "", "New permission (e.g., DENY)")

	// Add ACL subcommands
	cmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List ACLs",
			Run:   runACLList,
		},
		createCmd,
		modifyCmd,
		deleteCmd,
		getCmd,
	)

	return cmd
}

func newConsumerGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consumergroup",
		Short: "Manage Kafka consumer groups",
		Long:  `List, describe, and manage Kafka consumer groups.`,
	}

	// Add consumer group subcommands
	cmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List consumer groups",
			Run:   runConsumerGroupList,
		},
		&cobra.Command{
			Use:   "get [group-id]",
			Short: "Get consumer group details",
			Run:   runConsumerGroupGet,
		},
		&cobra.Command{
			Use:   "set-offsets [group-id] [topic] [partition] [offset]",
			Short: "Set consumer group offsets",
			Run:   runConsumerGroupSetOffsets,
		},
	)

	return cmd
}
