package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	// These variables are set during build time using -ldflags
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "Version: %s\n", version)
			fmt.Fprintf(cmd.OutOrStdout(), "Commit: %s\n", commit)
			fmt.Fprintf(cmd.OutOrStdout(), "Build Date: %s\n", buildDate)
		},
	}
}
