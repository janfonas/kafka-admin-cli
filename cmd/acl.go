package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/janfonas/kafka-admin-cli/internal/kafka"
	"github.com/spf13/cobra"
)

func runACLList(cmd *cobra.Command, args []string) {
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

	// List ACLs
	acls, err := client.ListAcls(ctx)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	// Print ACLs
	for _, acl := range acls {
		fmt.Fprintln(cmd.OutOrStdout(), acl)
	}
}

func runACLCreate(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Get flags
	resourceType, _ := cmd.Flags().GetString("resource-type")
	resourceName, _ := cmd.Flags().GetString("resource-name")
	principal, _ := cmd.Flags().GetString("principal")
	host, _ := cmd.Flags().GetString("host")
	operation, _ := cmd.Flags().GetString("operation")
	permission, _ := cmd.Flags().GetString("permission")

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

	// Create ACL
	err = client.CreateAcl(ctx, resourceType, resourceName, principal, host, operation, permission)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "ACL created successfully")
}

func runACLDelete(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Get flags
	resourceType, _ := cmd.Flags().GetString("resource-type")
	resourceName, _ := cmd.Flags().GetString("resource-name")
	principal, _ := cmd.Flags().GetString("principal")
	host, _ := cmd.Flags().GetString("host")
	operation, _ := cmd.Flags().GetString("operation")
	permission, _ := cmd.Flags().GetString("permission")

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

	// Delete ACL
	err = client.DeleteAcl(ctx, resourceType, resourceName, principal, host, operation, permission)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "ACL deleted successfully")
}

func runACLGet(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Get flags
	resourceType, _ := cmd.Flags().GetString("resource-type")
	resourceName, _ := cmd.Flags().GetString("resource-name")
	principal, _ := cmd.Flags().GetString("principal")

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

	// Get ACL details
	acls, err := client.GetAcl(ctx, resourceType, resourceName, principal)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	// Print ACL details
	for _, resource := range acls {
		fmt.Fprintf(cmd.OutOrStdout(), "Resource Type: %v\n", resource.ResourceType)
		fmt.Fprintf(cmd.OutOrStdout(), "Resource Name: %s\n", resource.ResourceName)
		fmt.Fprintln(cmd.OutOrStdout(), "ACLs:")
		for _, acl := range resource.ACLs {
			fmt.Fprintf(cmd.OutOrStdout(), "  Principal: %s\n", acl.Principal)
			fmt.Fprintf(cmd.OutOrStdout(), "  Host: %s\n", acl.Host)
			fmt.Fprintf(cmd.OutOrStdout(), "  Operation: %v\n", acl.Operation)
			fmt.Fprintf(cmd.OutOrStdout(), "  Permission Type: %v\n", acl.PermissionType)
			fmt.Fprintln(cmd.OutOrStdout())
		}
	}
}
