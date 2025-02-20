package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func executeCommand(root *cobra.Command, args ...string) (output string, err error) {
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs(args)

	err = root.Execute()
	return buf.String(), err
}

func TestRootCommand(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantErr     bool
		errContains string
		wantOutput  bool
	}{
		{
			name:       "version command",
			args:       []string{"version"},
			wantErr:    false,
			wantOutput: true,
		},
		{
			name:        "unknown command",
			args:        []string{"unknown"},
			wantErr:     true,
			errContains: "unknown command \"unknown\"",
			wantOutput:  false,
		},
		{
			name:       "help command",
			args:       []string{"help"},
			wantErr:    false,
			wantOutput: true,
		},
		{
			name:       "help topic command",
			args:       []string{"help", "topic"},
			wantErr:    false,
			wantOutput: true,
		},
		{
			name:       "help acl command",
			args:       []string{"help", "acl"},
			wantErr:    false,
			wantOutput: true,
		},
		{
			name:       "help consumergroup command",
			args:       []string{"help", "consumergroup"},
			wantErr:    false,
			wantOutput: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootCmd = NewRootCmd()
			initCommands()
			output, err := executeCommand(rootCmd, tt.args...)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if err != nil && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if tt.wantOutput && output == "" {
					t.Error("expected output got empty string")
				}
			}
		})
	}
}
