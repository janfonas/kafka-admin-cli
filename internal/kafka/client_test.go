package kafka

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
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

func TestTopicErrorHandling(t *testing.T) {
	tests := []struct {
		name      string
		errorCode int16
		wantError bool
		errorMsg  string
	}{
		{
			name:      "success",
			errorCode: 0,
			wantError: false,
		},
		{
			name:      "metadata update",
			errorCode: 7,
			wantError: false,
		},
		{
			name:      "topic exists",
			errorCode: 36,
			wantError: true,
			errorMsg:  "topic already exists: test-topic",
		},
		{
			name:      "invalid replication",
			errorCode: 37,
			wantError: true,
			errorMsg:  "invalid replication factor: 1",
		},
		{
			name:      "invalid partitions",
			errorCode: 39,
			wantError: true,
			errorMsg:  "invalid number of partitions: 1",
		},
		{
			name:      "invalid name",
			errorCode: 41,
			wantError: true,
			errorMsg:  "topic name is invalid",
		},
		{
			name:      "unknown error",
			errorCode: 99,
			wantError: true,
			errorMsg:  "failed to create topic: error code 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &kmsg.CreateTopicsResponse{
				Topics: []kmsg.CreateTopicsResponseTopic{
					{
						ErrorCode: tt.errorCode,
					},
				},
			}

			err := handleTopicCreateError(resp, "test-topic", 1, 1)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestACLErrorHandling(t *testing.T) {
	tests := []struct {
		name      string
		errorCode int16
		wantError bool
		errorMsg  string
	}{
		{
			name:      "success",
			errorCode: 0,
			wantError: false,
		},
		{
			name:      "metadata update",
			errorCode: 7,
			wantError: false,
		},
		{
			name:      "invalid resource",
			errorCode: 87,
			wantError: true,
			errorMsg:  "invalid resource type or name",
		},
		{
			name:      "invalid principal",
			errorCode: 88,
			wantError: true,
			errorMsg:  "invalid principal format",
		},
		{
			name:      "unknown error",
			errorCode: 50,
			wantError: true,
			errorMsg:  "failed to create ACL: error code 50",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &kmsg.CreateACLsResponse{
				Results: []kmsg.CreateACLsResponseResult{
					{
						ErrorCode: tt.errorCode,
					},
				},
			}

			err := handleACLCreateError(resp)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestConsumerGroupErrorHandling(t *testing.T) {
	tests := []struct {
		name      string
		errorCode int16
		wantError bool
		errorMsg  string
	}{
		{
			name:      "success",
			errorCode: 0,
			wantError: false,
		},
		{
			name:      "metadata update",
			errorCode: 7,
			wantError: false,
		},
		{
			name:      "group not found",
			errorCode: 15,
			wantError: true,
			errorMsg:  "consumer group not found",
		},
		{
			name:      "invalid group id",
			errorCode: 24,
			wantError: true,
			errorMsg:  "invalid consumer group id",
		},
		{
			name:      "unknown error",
			errorCode: 99,
			wantError: true,
			errorMsg:  "failed to process consumer group request: error code 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handleConsumerGroupError(tt.errorCode)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if err.Error() != tt.errorMsg {
					t.Errorf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}
