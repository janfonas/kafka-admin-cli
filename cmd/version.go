package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	gitCommit string
	buildDate string
)

func init() {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Version:    %s\n", version)
			if gitCommit != "" {
				fmt.Printf("Git commit: %s\n", gitCommit)
			}
			if buildDate != "" {
				fmt.Printf("Built:      %s\n", buildDate)
			}
			fmt.Printf("Go version: %s\n", runtime.Version())
			fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
		},
	}

	rootCmd.AddCommand(versionCmd)
}
