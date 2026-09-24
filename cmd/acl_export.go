package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	tlsAuthenticationExternal = "tls-external"
	tlsAuthenticationManaged  = "tls"
)

type aclExportOptions struct {
	tlsAuthentication string
	discoverSCRAM     bool
	scramCredentials  map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo
}

type scramCredentialDescriber interface {
	DescribeUserSCRAMs(context.Context, []string) (map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo, error)
}

func addACLExportFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("discover-scram", false, "Discover SCRAM-SHA-512 credentials for Strimzi ACL exports (requires Describe on Cluster)")
	cmd.Flags().String("tls-authentication", tlsAuthenticationExternal, "Authentication for CN principals in Strimzi exports (tls-external, tls)")
	_ = cmd.RegisterFlagCompletionFunc("tls-authentication", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return []string{tlsAuthenticationExternal, tlsAuthenticationManaged}, cobra.ShellCompDirectiveNoFileComp
	})
}

func (o aclExportOptions) validate() error {
	switch o.tlsAuthentication {
	case "", tlsAuthenticationExternal, tlsAuthenticationManaged:
		return nil
	default:
		return fmt.Errorf("invalid --tls-authentication %q: use tls-external or tls", o.tlsAuthentication)
	}
}

func readACLExportOptions(cmd *cobra.Command, outputFormat string) (aclExportOptions, error) {
	var options aclExportOptions
	var err error
	options.tlsAuthentication, err = cmd.Flags().GetString("tls-authentication")
	if err != nil {
		return options, err
	}
	if options.tlsAuthentication == "" {
		return options, fmt.Errorf("invalid --tls-authentication: use tls-external or tls")
	}
	options.discoverSCRAM, err = cmd.Flags().GetBool("discover-scram")
	if err != nil {
		return options, err
	}
	if err := options.validate(); err != nil {
		return options, err
	}
	if outputFormat != outputStrimzi && (options.discoverSCRAM || cmd.Flags().Changed("tls-authentication")) {
		return options, fmt.Errorf("--discover-scram and --tls-authentication require --output strimzi")
	}
	return options, nil
}

func exportACLStrimzi(ctx context.Context, w io.Writer, client scramCredentialDescriber, resources []kmsg.DescribeACLsResponseResource, options aclExportOptions) error {
	if err := options.validate(); err != nil {
		return err
	}
	if options.discoverSCRAM {
		var users []string
		seen := make(map[string]bool)
		for _, resource := range resources {
			for _, acl := range resource.ACLs {
				name, authentication, err := strimziUserIdentity(acl.Principal, options.tlsAuthentication)
				if err != nil {
					return err
				}
				if authentication == nil && !seen[name] {
					users = append(users, name)
					seen[name] = true
				}
			}
		}
		// An empty Kafka SCRAM query can mean all users. Only query exported non-TLS identities.
		if len(users) > 0 {
			credentials, err := client.DescribeUserSCRAMs(ctx, users)
			if err != nil {
				return fmt.Errorf("cannot discover SCRAM authentication for Strimzi export: %w", err)
			}
			options.scramCredentials = credentials
		}
	}
	return formatACLStrimzi(w, resources, options)
}

func discoveredSCRAMAuthentication(user string, credentials map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo) (*strimziUserAuthentication, error) {
	infos, found := credentials[user]
	if !found {
		return nil, fmt.Errorf("SCRAM discovery returned no result for user %q", user)
	}
	if len(infos) == 0 {
		return nil, nil
	}
	for _, info := range infos {
		if info.Mechanism == 2 {
			return &strimziUserAuthentication{Type: "scram-sha-512"}, nil
		}
	}
	return nil, fmt.Errorf("cannot export SCRAM authentication for user %q: no SCRAM-SHA-512 credential was found; Strimzi does not support other SCRAM mechanisms", user)
}
