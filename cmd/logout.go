package cmd

import (
	"fmt"

	"github.com/janfonas/kafka-admin-cli/internal/credentials"
	"github.com/spf13/cobra"
)

var (
	logoutProfile string
)

func newLogoutCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Remove stored credentials",
		Long: `Remove stored Kafka connection credentials from your system's secure keyring.

Examples:
  # Logout from default profile
  kac logout

  # Logout from named profile
  kac logout --profile prod`,
		RunE: runLogout,
	}

	cmd.Flags().StringVar(&logoutProfile, "profile", "default", "Profile name to remove")

	return cmd
}

func runLogout(cmd *cobra.Command, args []string) error {
	err := credentials.Delete(logoutProfile)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully removed credentials for profile '%s'\n", logoutProfile)
	return nil
}
