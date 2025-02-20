package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	Brokers       string
	Username      string
	Password      string
	SASLMechanism string
	CACertPath    string
	Insecure      bool
}

func LoadConfig() (*Config, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS is required")
	}

	username := os.Getenv("KAFKA_USERNAME")
	if username == "" {
		return nil, fmt.Errorf("KAFKA_USERNAME is required")
	}

	password := os.Getenv("KAFKA_PASSWORD")
	if password == "" {
		return nil, fmt.Errorf("KAFKA_PASSWORD is required")
	}

	saslMechanism := os.Getenv("KAFKA_SASL_MECHANISM")
	if saslMechanism == "" {
		saslMechanism = "SCRAM-SHA-512"
	}

	// Validate SASL mechanism
	switch strings.ToUpper(saslMechanism) {
	case "SCRAM-SHA-512", "PLAIN":
		// Valid mechanisms
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", saslMechanism)
	}

	caCertPath := os.Getenv("KAFKA_CA_CERT")
	insecure := strings.ToLower(os.Getenv("KAFKA_INSECURE")) == "true"

	return &Config{
		Brokers:       brokers,
		Username:      username,
		Password:      password,
		SASLMechanism: saslMechanism,
		CACertPath:    caCertPath,
		Insecure:      insecure,
	}, nil
}
