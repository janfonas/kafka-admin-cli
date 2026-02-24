package cmd

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/janfonas/kafka-admin-cli/internal/credentials"
	"github.com/spf13/cobra"
)

func newProfileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage credential profiles",
		Long: `Manage stored credential profiles.
Profiles allow you to store multiple sets of Kafka connection credentials
and switch between them easily.`,
	}

	cmd.AddCommand(
		newProfileListCmd(),
		newProfileSwitchCmd(),
	)

	return cmd
}

func newProfileListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all stored profiles",
		Long: `List all stored credential profiles.
Shows profile names, brokers, usernames, and which profile is currently active.

Examples:
  # List all profiles
  kac profile list`,
		RunE: runProfileList,
	}

	return cmd
}

func newProfileSwitchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "switch <profile-name>",
		Short: "Switch to a different profile",
		Long: `Set the active profile.
The active profile will be used by default for all commands unless overridden
with the --profile flag.

Examples:
  # Switch to production profile
  kac profile switch prod

  # Switch to default profile
  kac profile switch default`,
		Args:              cobra.ExactArgs(1),
		RunE:              runProfileSwitch,
		ValidArgsFunction: completeProfileNames,
	}

	return cmd
}

func runProfileList(cmd *cobra.Command, args []string) error {
	profiles, err := credentials.List()
	if err != nil {
		return fmt.Errorf("failed to list profiles: %w", err)
	}

	if len(profiles) == 0 {
		fmt.Println("No profiles found. Use 'kac login' to create a profile.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ACTIVE\tNAME\tBROKERS\tUSERNAME\tSASL\tTLS")

	for _, p := range profiles {
		activeMarker := " "
		if p.IsActive {
			activeMarker = "*"
		}

		sasl := p.SASLMechanism
		if sasl == "" {
			sasl = "SCRAM-SHA-512"
		}

		tls := "yes"
		if p.Insecure {
			tls = "insecure"
		} else if p.CACertPath != "" {
			tls = "custom CA"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			activeMarker,
			p.Name,
			p.Brokers,
			p.Username,
			sasl,
			tls,
		)
	}

	w.Flush()
	return nil
}

func runProfileSwitch(cmd *cobra.Command, args []string) error {
	profileName := args[0]

	err := credentials.SetActiveProfile(profileName)
	if err != nil {
		return err
	}

	fmt.Printf("Switched to profile '%s'\n", profileName)
	return nil
}
