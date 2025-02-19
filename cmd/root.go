package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	brokers       []string
	username      string
	password      string
	caCertPath    string
	saslMechanism string
	insecure      bool
)

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
	rootCmd.PersistentFlags().StringVarP(&password, "password", "w", "", "SASL password")
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
