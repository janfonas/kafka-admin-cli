package kafka

import (
	"strings"
	"testing"
)

func TestBrokerURLParsing(t *testing.T) {
	tests := []struct {
		name     string
		brokers  []string
		expected []string
	}{
		{
			name:     "default port",
			brokers:  []string{"kafka1", "kafka2"},
			expected: []string{"kafka1:9092", "kafka2:9092"},
		},
		{
			name:     "custom port",
			brokers:  []string{"kafka1:9093", "kafka2:9094"},
			expected: []string{"kafka1:9093", "kafka2:9094"},
		},
		{
			name:     "mixed ports",
			brokers:  []string{"kafka1:9093", "kafka2"},
			expected: []string{"kafka1:9093", "kafka2:9092"},
		},
		{
			name:     "with domain",
			brokers:  []string{"kafka.example.com:443"},
			expected: []string{"kafka.example.com:443"},
		},
		{
			name:     "with IPv4",
			brokers:  []string{"192.168.1.1", "192.168.1.2:9093"},
			expected: []string{"192.168.1.1:9092", "192.168.1.2:9093"},
		},
		{
			name:     "with IPv6",
			brokers:  []string{"[::1]", "[::1]:9093"},
			expected: []string{"[::1]:9092", "[::1]:9093"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seeds := make([]string, len(tt.brokers))
			for i, broker := range tt.brokers {
				u, err := parseURL(broker)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				seeds[i] = u
			}

			for i, expected := range tt.expected {
				if seeds[i] != expected {
					t.Errorf("expected %q, got %q", expected, seeds[i])
				}
			}
		})
	}
}

func TestBrokerURLParsingErrors(t *testing.T) {
	tests := []struct {
		name    string
		broker  string
		wantErr bool
	}{
		{
			name:    "empty broker",
			broker:  "",
			wantErr: true,
		},
		{
			name:    "invalid port",
			broker:  "kafka1:abc",
			wantErr: true,
		},
		{
			name:    "invalid IPv6",
			broker:  "[::1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseURL(tt.broker)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseURL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSASLMechanismValidation(t *testing.T) {
	tests := []struct {
		name        string
		mechanism   string
		wantErr     bool
		errContains string
	}{
		{
			name:      "SCRAM-SHA-512",
			mechanism: "SCRAM-SHA-512",
			wantErr:   false,
		},
		{
			name:      "PLAIN",
			mechanism: "PLAIN",
			wantErr:   false,
		},
		{
			name:        "invalid mechanism",
			mechanism:   "INVALID",
			wantErr:     true,
			errContains: "unsupported SASL mechanism",
		},
		{
			name:        "empty mechanism",
			mechanism:   "",
			wantErr:     true,
			errContains: "unsupported SASL mechanism",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSASLMechanism(tt.mechanism)
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
			}
		})
	}
}
