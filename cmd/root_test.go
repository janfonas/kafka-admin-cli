package cmd

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/janfonas/kafka-admin-cli/internal/credentials"
	"github.com/spf13/cobra"
	"github.com/zalando/go-keyring"
)

func executeCommand(root *cobra.Command, args ...string) (output string, err error) {
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs(args)

	err = root.Execute()
	return buf.String(), err
}

func isolatedRootCommand(t *testing.T) *cobra.Command {
	t.Helper()
	previousRoot := rootCmd
	previousProfile, previousPrompt := profile, promptPassword
	previous := credentials.Profile{
		Brokers: brokers, Username: username, Password: password,
		SASLMechanism: saslMechanism, CACertPath: caCertPath, Insecure: insecure,
	}
	t.Setenv("HOME", t.TempDir())
	keyring.MockInit()
	t.Cleanup(func() {
		rootCmd = previousRoot
		profile, promptPassword = previousProfile, previousPrompt
		brokers, username, password = previous.Brokers, previous.Username, previous.Password
		saslMechanism, caCertPath, insecure = previous.SASLMechanism, previous.CACertPath, previous.Insecure
		keyring.MockInit()
	})
	rootCmd = NewRootCmd()
	initCommands()
	return rootCmd
}

func TestProfileFlagPrecedence(t *testing.T) {
	for _, testcase := range []struct {
		name     string
		args     []string
		override func(*credentials.Profile)
		active   bool
	}{
		{name: "profile defaults"},
		{name: "active profile", active: true},
		{name: "explicit TLS verification", args: []string{"--insecure=false"}, override: func(want *credentials.Profile) { want.Insecure = false }},
		{name: "explicit default mechanism", args: []string{"--sasl-mechanism=SCRAM-SHA-512"}, override: func(want *credentials.Profile) { want.SASLMechanism = "SCRAM-SHA-512" }},
		{name: "partial broker override", args: []string{"--brokers=override.invalid:9093"}, override: func(want *credentials.Profile) { want.Brokers = "override.invalid:9093" }},
		{name: "partial username override", args: []string{"--username=other"}, override: func(want *credentials.Profile) { want.Username = "other" }},
		{name: "partial password override", args: []string{"--password=override"}, override: func(want *credentials.Profile) { want.Password = "override" }},
		{name: "explicit empty CA", args: []string{"--ca-cert="}, override: func(want *credentials.Profile) { want.CACertPath = "" }},
		{name: "explicit empty username", args: []string{"--username="}, override: func(want *credentials.Profile) { want.Username = "" }},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			command := isolatedRootCommand(t)
			stored := credentials.Profile{
				Brokers: "unused.invalid:9093", Username: "review-user", Password: "synthetic",
				SASLMechanism: "PLAIN", CACertPath: "/synthetic/ca.pem", Insecure: true,
			}
			if err := credentials.Store("review", &stored); err != nil {
				t.Fatal(err)
			}
			arguments := []string{"--profile=review"}
			if testcase.active {
				if err := credentials.SetActiveProfile("review"); err != nil {
					t.Fatal(err)
				}
				arguments = nil
			}
			arguments = append(arguments, testcase.args...)
			arguments = append(arguments, "version")
			if _, err := executeCommand(command, arguments...); err != nil {
				t.Fatal(err)
			}
			want := stored
			if testcase.override != nil {
				testcase.override(&want)
			}
			got := credentials.Profile{
				Brokers: brokers, Username: username, Password: password,
				SASLMechanism: saslMechanism, CACertPath: caCertPath, Insecure: insecure,
			}
			if got != want {
				t.Errorf("profile/flag precedence mismatch: got %+v, want %+v", got, want)
			}
		})
	}
}

func TestMissingProfile(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		command := isolatedRootCommand(t)
		arguments := []string{"--brokers=unused.invalid:9093", "--username=synthetic", "--password=synthetic", "version"}
		if explicit {
			arguments = append([]string{"--profile=missing"}, arguments...)
		}
		_, err := executeCommand(command, arguments...)
		if explicit && (err == nil || !strings.Contains(err.Error(), "not found")) {
			t.Fatalf("expected explicitly missing profile error, got %v", err)
		}
		if !explicit && err != nil {
			t.Fatalf("implicit missing profile should allow explicit credentials: %v", err)
		}
	}
}

func TestCommandExitStatus(t *testing.T) {
	if os.Getenv("KAC_TEST_PROCESS") == "1" {
		keyring.MockInit()
		rootCmd = NewRootCmd()
		initCommands()
		rootCmd.SetArgs(os.Args[3:])
		Execute()
		os.Exit(0)
	}
	for _, testcase := range []struct {
		name string
		args []string
		want string
	}{
		{name: "create topic", args: []string{"create", "topic", "events"}, want: "failed to read CA certificate"},
		{name: "delete topic", args: []string{"delete", "topic", "events"}, want: "failed to read CA certificate"},
		{name: "modify topic", args: []string{"modify", "topic", "events", "--config=retention.ms=1000"}, want: "failed to read CA certificate"},
		{name: "create ACL", args: []string{"create", "acl"}, want: "failed to read CA certificate"},
		{name: "delete ACL", args: []string{"delete", "acl"}, want: "failed to read CA certificate"},
		{name: "modify ACL", args: []string{"modify", "acl"}, want: "failed to read CA certificate"},
		{name: "list groups", args: []string{"get", "consumergroups"}, want: "failed to read CA certificate"},
		{name: "get group", args: []string{"get", "consumergroup", "review"}, want: "failed to read CA certificate"},
		{name: "delete group", args: []string{"delete", "consumergroup", "review"}, want: "failed to read CA certificate"},
		{name: "set offsets", args: []string{"set-offsets", "cg", "review", "events", "0", "42"}, want: "failed to read CA certificate"},
		{name: "invalid partition", args: []string{"set-offsets", "cg", "review", "events", "invalid", "42"}, want: "invalid partition"},
		{name: "invalid offset", args: []string{"set-offsets", "cg", "review", "events", "0", "invalid"}, want: "invalid offset"},
		{name: "invalid config", args: []string{"modify", "topic", "events", "--config=invalid"}, want: "invalid config format"},
		{name: "missing config", args: []string{"modify", "topic", "events"}, want: "at least one config parameter"},
		{name: "success", args: []string{"version"}},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			home := t.TempDir()
			arguments := []string{"-test.run=^TestCommandExitStatus$", "--",
				"--brokers=unused.invalid:9093", "--username=synthetic", "--password=synthetic",
				"--ca-cert=" + filepath.Join(home, "missing.pem"),
			}
			process := exec.Command(os.Args[0], append(arguments, testcase.args...)...)
			process.Env = append(os.Environ(), "KAC_TEST_PROCESS=1", "HOME="+home)
			var stdout, stderr bytes.Buffer
			process.Stdout, process.Stderr = &stdout, &stderr
			err := process.Run()
			if testcase.want == "" {
				if err != nil || stderr.Len() != 0 || stdout.Len() == 0 {
					t.Fatalf("expected successful output: err=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
				}
				return
			}
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
				t.Fatalf("expected exit 1, got %v; stderr=%q", err, stderr.String())
			}
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), testcase.want) || strings.Count(stderr.String(), "\n") != 1 {
				t.Fatalf("expected one error on stderr only, got stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
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
