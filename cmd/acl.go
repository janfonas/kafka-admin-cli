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
		// For strimzi output, fetch full ACL details instead of just principals
		acls, err := client.GetAcl(ctx, "", "", "")
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
		formatACLStrimzi(cmd.OutOrStdout(), acls)
	default:
		// List ACLs (principals only)
		acls, err := client.ListAcls(ctx)
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
		for _, acl := range acls {
			fmt.Fprintln(cmd.OutOrStdout(), acl)
		}
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

func runACLModify(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Get flags
	resourceType, _ := cmd.Flags().GetString("resource-type")
	resourceName, _ := cmd.Flags().GetString("resource-name")
	principal, _ := cmd.Flags().GetString("principal")
	host, _ := cmd.Flags().GetString("host")
	operation, _ := cmd.Flags().GetString("operation")
	permission, _ := cmd.Flags().GetString("permission")
	newPermission, _ := cmd.Flags().GetString("new-permission")

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

	// Modify ACL
	err = client.ModifyAcl(ctx, resourceType, resourceName, principal, host, operation, permission, newPermission)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	fmt.Fprintln(cmd.OutOrStdout(), "ACL modified successfully")
}

func runACLGet(cmd *cobra.Command, args []string) {
	ctx := context.Background()

	// Get flags
	resourceType, _ := cmd.Flags().GetString("resource-type")
	resourceName, _ := cmd.Flags().GetString("resource-name")
	principal, _ := cmd.Flags().GetString("principal")
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

	// Get ACL details
	acls, err := client.GetAcl(ctx, resourceType, resourceName, principal)
	if err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	switch outputFormat {
	case outputStrimzi:
		formatACLStrimzi(cmd.OutOrStdout(), acls)
	default:
		formatACLTable(cmd.OutOrStdout(), acls)
	}
}
