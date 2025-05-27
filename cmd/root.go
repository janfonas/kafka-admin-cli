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
		newGetCmd(),
		newCreateCmd(),
		newDeleteCmd(),
		newModifyCmd(),
		newSetOffsetsCmd(),
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
