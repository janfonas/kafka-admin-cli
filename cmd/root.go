package cmd

import (
	"fmt"
	"os"
	"syscall"

	"github.com/janfonas/kafka-admin-cli/internal/credentials"
	"github.com/spf13/cobra"
	"golang.org/x/term"
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
	profile        string
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
		newLoginCmd(),
		newLogoutCmd(),
		newProfileCmd(),
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
			// Skip credential loading for login/logout/profile commands
			cmdName := cmd.Name()
			parentName := ""
			if cmd.Parent() != nil {
				parentName = cmd.Parent().Name()
			}

			if cmdName == "login" || cmdName == "logout" || cmdName == "profile" || parentName == "profile" {
				return nil
			}

			// If no profile flag provided, use active profile
			if !cmd.Flags().Changed("profile") {
				activeProfile := credentials.GetActiveProfile()
				if activeProfile != "" && activeProfile != "default" {
					profile = activeProfile
				}
			}

			if err := loadCredentialsFromProfile(cmd); err != nil {
				if cmd.Flags().Changed("profile") {
					return err
				}
			}

			// Handle password prompting
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
	cmd.PersistentFlags().StringVar(&profile, "profile", "default", "Profile name to use for stored credentials")
	cmd.PersistentFlags().StringVarP(&brokers, "brokers", "b", "", "Kafka broker list (comma-separated)")
	cmd.PersistentFlags().StringVarP(&username, "username", "u", "", "SASL username")
	cmd.PersistentFlags().StringVarP(&password, "password", "w", "", "SASL password")
	cmd.PersistentFlags().BoolVarP(&promptPassword, "prompt-password", "P", false, "Prompt for password")
	cmd.PersistentFlags().StringVar(&saslMechanism, "sasl-mechanism", "SCRAM-SHA-512", "SASL mechanism (SCRAM-SHA-512 or PLAIN)")
	cmd.PersistentFlags().StringVar(&caCertPath, "ca-cert", "", "CA certificate file path")
	cmd.PersistentFlags().BoolVar(&insecure, "insecure", false, "Skip TLS certificate verification")

	registerProfileFlagCompletion(cmd)
	registerSASLMechanismCompletion(cmd)

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
	passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Fprintln(os.Stderr)
	return string(passwordBytes), nil
}

func loadCredentialsFromProfile(cmd *cobra.Command) error {
	prof, err := credentials.Load(profile)
	if err != nil {
		return err
	}

	// Only set values that weren't provided via flags
	if !cmd.Flags().Changed("brokers") {
		brokers = prof.Brokers
	}
	if !cmd.Flags().Changed("username") {
		username = prof.Username
	}
	if !cmd.Flags().Changed("password") {
		password = prof.Password
	}
	if !cmd.Flags().Changed("sasl-mechanism") && prof.SASLMechanism != "" {
		saslMechanism = prof.SASLMechanism
	}
	if !cmd.Flags().Changed("ca-cert") {
		caCertPath = prof.CACertPath
	}
	if !cmd.Flags().Changed("insecure") {
		insecure = prof.Insecure
	}

	return nil
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
