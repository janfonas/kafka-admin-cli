package cmd

import (
	"bufio"
	"fmt"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var (
	brokers       []string
	username      string
	password      string
	promptPass    bool
	caCertPath    string
	saslMechanism string
	insecure      bool
)

func readPassword() (string, error) {
	if promptPass {
		// Check if there's data available on stdin
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			// Read from stdin (pipe)
			scanner := bufio.NewScanner(os.Stdin)
			if scanner.Scan() {
				return scanner.Text(), nil
			}
			if err := scanner.Err(); err != nil {
				return "", fmt.Errorf("failed to read password from stdin: %w", err)
			}
			return "", fmt.Errorf("no password provided on stdin")
		}

		// No pipe, prompt for password
		fmt.Print("Enter password: ")
		passBytes, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Println() // Add newline after password input
		if err != nil {
			return "", fmt.Errorf("failed to read password: %w", err)
		}
		return string(passBytes), nil
	}
	return password, nil
}

func getPassword() (string, error) {
	if promptPass && password != "" {
		return "", fmt.Errorf("cannot use both --password and --prompt-password")
	}
	return readPassword()
}

var version = "dev"

var rootCmd = &cobra.Command{
	Use:     "kac",
	Version: version,
	Short:   "Kafka Admin Client - A CLI application to manage Kafka topics and ACLs",
	Long: `Kafka Admin Client - A CLI application to manage Kafka topics and ACLs using the Franz-go Kafka client.
Written by Jan Harald Fonås with the help of an LLM.`,
}

func init() {
	rootCmd.PersistentFlags().StringSliceVarP(&brokers, "brokers", "b", []string{"localhost:9092"}, "Kafka broker list (comma-separated)")
	rootCmd.PersistentFlags().StringVarP(&username, "username", "u", "", "SASL username")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "w", "", "SASL password (use -P to prompt for password)")
	rootCmd.PersistentFlags().BoolVarP(&promptPass, "prompt-password", "P", false, "Prompt for password or read from stdin")
	rootCmd.PersistentFlags().StringVar(&caCertPath, "ca-cert", "", "Path to CA certificate file for TLS connections")
	rootCmd.PersistentFlags().StringVar(&saslMechanism, "sasl-mechanism", "SCRAM-SHA-512", "SASL mechanism (SCRAM-SHA-512 or PLAIN)")
	rootCmd.PersistentFlags().BoolVar(&insecure, "insecure", false, "Skip TLS certificate verification")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
