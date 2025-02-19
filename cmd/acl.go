package cmd

import (
	"context"
	"fmt"

	"github.com/a00262/kafka-admin-cli/internal/kafka"

	"github.com/spf13/cobra"
)

var (
	resourceType string
	resourceName string
	principal    string
	host         string
	operation    string
	permission   string
)

func init() {
	// Create ACL command
	createAclCmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new ACL",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism, insecure)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			err = client.CreateAcl(context.Background(), resourceType, resourceName, principal, host, operation, permission)
			if err != nil {
				return fmt.Errorf("failed to create ACL: %w", err)
			}

			fmt.Println("ACL created successfully")
			return nil
		},
	}
	createAclCmd.Flags().StringVar(&resourceType, "resource-type", "", "Resource type (e.g., TOPIC, GROUP)")
	createAclCmd.Flags().StringVar(&resourceName, "resource-name", "", "Resource name")
	createAclCmd.Flags().StringVar(&principal, "principal", "", "Principal (e.g., User:alice)")
	createAclCmd.Flags().StringVar(&host, "host", "*", "Host")
	createAclCmd.Flags().StringVar(&operation, "operation", "", "Operation (e.g., READ, WRITE)")
	createAclCmd.Flags().StringVar(&permission, "permission", "", "Permission (e.g., ALLOW, DENY)")
	createAclCmd.MarkFlagRequired("resource-type")
	createAclCmd.MarkFlagRequired("resource-name")
	createAclCmd.MarkFlagRequired("principal")
	createAclCmd.MarkFlagRequired("operation")
	createAclCmd.MarkFlagRequired("permission")

	// Delete ACL command
	deleteAclCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete an ACL",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism, insecure)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			err = client.DeleteAcl(context.Background(), resourceType, resourceName, principal, host, operation, permission)
			if err != nil {
				return fmt.Errorf("failed to delete ACL: %w", err)
			}

			fmt.Println("ACL deleted successfully")
			return nil
		},
	}
	deleteAclCmd.Flags().StringVar(&resourceType, "resource-type", "", "Resource type (e.g., TOPIC, GROUP)")
	deleteAclCmd.Flags().StringVar(&resourceName, "resource-name", "", "Resource name")
	deleteAclCmd.Flags().StringVar(&principal, "principal", "", "Principal (e.g., User:alice)")
	deleteAclCmd.Flags().StringVar(&host, "host", "*", "Host")
	deleteAclCmd.Flags().StringVar(&operation, "operation", "", "Operation (e.g., READ, WRITE)")
	deleteAclCmd.Flags().StringVar(&permission, "permission", "", "Permission (e.g., ALLOW, DENY)")
	deleteAclCmd.MarkFlagRequired("resource-type")
	deleteAclCmd.MarkFlagRequired("resource-name")
	deleteAclCmd.MarkFlagRequired("principal")
	deleteAclCmd.MarkFlagRequired("operation")
	deleteAclCmd.MarkFlagRequired("permission")

	// List ACLs command
	listAclsCmd := &cobra.Command{
		Use:   "list",
		Short: "List all ACLs",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism, insecure)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			users, err := client.ListAcls(context.Background())
			if err != nil {
				return fmt.Errorf("failed to list users: %w", err)
			}

			if len(users) == 0 {
				fmt.Println("No users found")
				return nil
			}

			fmt.Println("Users:")
			for _, user := range users {
				fmt.Printf("- %s\n", user)
			}
			return nil
		},
	}

	// Get ACL command
	getAclCmd := &cobra.Command{
		Use:   "get",
		Short: "Get ACL details",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := kafka.NewClient(brokers, username, password, caCertPath, saslMechanism, insecure)
			if err != nil {
				return fmt.Errorf("failed to create Kafka client: %w", err)
			}
			defer client.Close()

			acls, err := client.GetAcl(context.Background(), resourceType, resourceName, principal)
			if err != nil {
				return fmt.Errorf("failed to get ACL details: %w", err)
			}

			fmt.Printf("ACLs for Resource Type: %s, Resource Name: %s, Principal: %s\n\n", resourceType, resourceName, principal)
			for _, acl := range acls {
				fmt.Printf("Resource Type: %v\n", acl.ResourceType)
				fmt.Printf("Resource Name: %v\n", acl.ResourceName)
				for _, entry := range acl.ACLs {
					fmt.Printf("  Principal: %v\n", entry.Principal)
					fmt.Printf("  Host: %v\n", entry.Host)
					fmt.Printf("  Operation: %v\n", entry.Operation)
					fmt.Printf("  Permission Type: %v\n", entry.PermissionType)
					fmt.Println()
				}
			}
			return nil
		},
	}
	getAclCmd.Flags().StringVar(&resourceType, "resource-type", "", "Resource type (e.g., TOPIC, GROUP)")
	getAclCmd.Flags().StringVar(&resourceName, "resource-name", "", "Resource name")
	getAclCmd.Flags().StringVar(&principal, "principal", "", "Principal (e.g., User:alice)")
	getAclCmd.MarkFlagRequired("resource-type")
	getAclCmd.MarkFlagRequired("resource-name")
	getAclCmd.MarkFlagRequired("principal")

	// ACL command
	aclCmd := &cobra.Command{
		Use:   "acl",
		Short: "Manage Kafka ACLs",
	}
	aclCmd.AddCommand(createAclCmd)
	aclCmd.AddCommand(deleteAclCmd)
	aclCmd.AddCommand(listAclsCmd)
	aclCmd.AddCommand(getAclCmd)

	rootCmd.AddCommand(aclCmd)
}
