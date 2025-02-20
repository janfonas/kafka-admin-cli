package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name        string
		envVars     map[string]string
		wantErr     bool
		errContains string
	}{
		{
			name: "valid config",
			envVars: map[string]string{
				"KAFKA_BROKERS":        "localhost:9092",
				"KAFKA_USERNAME":       "user",
				"KAFKA_PASSWORD":       "pass",
				"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
			},
			wantErr: false,
		},
		{
			name: "missing brokers",
			envVars: map[string]string{
				"KAFKA_USERNAME":       "user",
				"KAFKA_PASSWORD":       "pass",
				"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
			},
			wantErr:     true,
			errContains: "KAFKA_BROKERS is required",
		},
		{
			name: "missing username",
			envVars: map[string]string{
				"KAFKA_BROKERS":        "localhost:9092",
				"KAFKA_PASSWORD":       "pass",
				"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
			},
			wantErr:     true,
			errContains: "KAFKA_USERNAME is required",
		},
		{
			name: "missing password",
			envVars: map[string]string{
				"KAFKA_BROKERS":        "localhost:9092",
				"KAFKA_USERNAME":       "user",
				"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
			},
			wantErr:     true,
			errContains: "KAFKA_PASSWORD is required",
		},
		{
			name: "invalid SASL mechanism",
			envVars: map[string]string{
				"KAFKA_BROKERS":        "localhost:9092",
				"KAFKA_USERNAME":       "user",
				"KAFKA_PASSWORD":       "pass",
				"KAFKA_SASL_MECHANISM": "INVALID",
			},
			wantErr:     true,
			errContains: "unsupported SASL mechanism",
		},
		{
			name: "default SASL mechanism",
			envVars: map[string]string{
				"KAFKA_BROKERS":  "localhost:9092",
				"KAFKA_USERNAME": "user",
				"KAFKA_PASSWORD": "pass",
			},
			wantErr: false,
		},
		{
			name: "with CA cert",
			envVars: map[string]string{
				"KAFKA_BROKERS":        "localhost:9092",
				"KAFKA_USERNAME":       "user",
				"KAFKA_PASSWORD":       "pass",
				"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
				"KAFKA_CA_CERT":        "/path/to/ca.crt",
			},
			wantErr: false,
		},
		{
			name: "with insecure",
			envVars: map[string]string{
				"KAFKA_BROKERS":        "localhost:9092",
				"KAFKA_USERNAME":       "user",
				"KAFKA_PASSWORD":       "pass",
				"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
				"KAFKA_INSECURE":       "true",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear environment
			os.Clearenv()

			// Set test environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			cfg, err := LoadConfig()
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if err != nil && !contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				// Verify config values
				if cfg.Brokers == "" {
					t.Error("brokers is empty")
				}
				if cfg.Username == "" {
					t.Error("username is empty")
				}
				if cfg.Password == "" {
					t.Error("password is empty")
				}
				if cfg.SASLMechanism == "" {
					t.Error("SASL mechanism is empty")
				}

				// Verify default SASL mechanism
				if tt.envVars["KAFKA_SASL_MECHANISM"] == "" && cfg.SASLMechanism != "SCRAM-SHA-512" {
					t.Errorf("expected default SASL mechanism SCRAM-SHA-512, got %s", cfg.SASLMechanism)
				}

				// Verify CA cert path
				if tt.envVars["KAFKA_CA_CERT"] != "" && cfg.CACertPath != tt.envVars["KAFKA_CA_CERT"] {
					t.Errorf("expected CA cert path %s, got %s", tt.envVars["KAFKA_CA_CERT"], cfg.CACertPath)
				}

				// Verify insecure flag
				if tt.envVars["KAFKA_INSECURE"] == "true" && !cfg.Insecure {
					t.Error("expected insecure to be true")
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
