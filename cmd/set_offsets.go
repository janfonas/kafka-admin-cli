package cmd

import (
	"github.com/spf13/cobra"
)

func newSetOffsetsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-offsets",
		Short: "Set consumer group offsets",
		Long:  `Set offsets for consumer groups.`,
	}

	// Add subcommands
	cmd.AddCommand(
		newSetOffsetsConsumerGroupCmd(),
	)

	return cmd
}

// Set offsets for consumer group
func newSetOffsetsConsumerGroupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "consumergroup [group-id] [topic] [partition] [offset]",
		Aliases: []string{"cg"},
		Short:   "Set consumer group offsets",
		Args:    cobra.ExactArgs(4),
		Run:     runConsumerGroupSetOffsets,
	}
	return cmd
}
