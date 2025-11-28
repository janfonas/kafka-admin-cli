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
	loginProfile string
)

func newLoginCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Store Kafka connection credentials securely",
		Long: `Store Kafka connection credentials in your system's secure keyring.
Credentials are encrypted by your operating system (Keychain on macOS, 
Secret Service on Linux, Credential Manager on Windows).

Examples:
  # Login with default profile
  kac login --brokers kafka1:9092 --username alice

  # Login with named profile
  kac login --profile prod --brokers kafka1:9092 --username alice

  # Login with all options
  kac login --brokers kafka1:9092 --username alice --sasl-mechanism PLAIN --ca-cert /path/to/ca.crt`,
		RunE: runLogin,
	}

	cmd.Flags().StringVar(&loginProfile, "profile", "default", "Profile name to store credentials under")

	return cmd
}

func runLogin(cmd *cobra.Command, args []string) error {
	// Check if required flags are provided
	if brokers == "" {
		return fmt.Errorf("--brokers is required")
	}
	if username == "" {
		return fmt.Errorf("--username is required")
	}

	// Get password
	var pwd string
	var err error

	if password != "" {
		pwd = password
	} else {
		fmt.Fprint(os.Stderr, "Password: ")
		passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return fmt.Errorf("failed to read password: %w", err)
		}
		fmt.Fprintln(os.Stderr) // New line after password input
		pwd = string(passwordBytes)
	}

	if pwd == "" {
		return fmt.Errorf("password cannot be empty")
	}

	// Create profile
	profile := &credentials.Profile{
		Brokers:       brokers,
		Username:      username,
		Password:      pwd,
		SASLMechanism: saslMechanism,
		CACertPath:    caCertPath,
		Insecure:      insecure,
	}

	// Store in keyring
	err = credentials.Store(loginProfile, profile)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully stored credentials for profile '%s'\n", loginProfile)
	return nil
}
