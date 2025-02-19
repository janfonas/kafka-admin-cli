package kafka

import (
	"fmt"
	"net/url"
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

// Helper function to extract URL parsing logic for testing
func parseURL(broker string) (string, error) {
	u, err := url.Parse("//" + broker)
	if err != nil {
		return "", err
	}

	if u.Port() == "" {
		return fmt.Sprintf("%s:9092", u.Hostname()), nil
	}
	return fmt.Sprintf("%s:%s", u.Hostname(), u.Port()), nil
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

// Helper function to extract error handling logic for testing
func handleTopicCreateError(resp *kmsg.CreateTopicsResponse, topic string, partitions, replicationFactor int) error {
	if len(resp.Topics) > 0 && resp.Topics[0].ErrorCode != 0 {
		switch resp.Topics[0].ErrorCode {
		case 7:
			return nil
		case 36:
			return fmt.Errorf("topic already exists: %s", topic)
		case 37:
			return fmt.Errorf("invalid replication factor: %d", replicationFactor)
		case 39:
			return fmt.Errorf("invalid number of partitions: %d", partitions)
		case 41:
			return fmt.Errorf("topic name is invalid")
		default:
			return fmt.Errorf("failed to create topic: error code %v", resp.Topics[0].ErrorCode)
		}
	}
	return nil
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

// Helper function to extract error handling logic for testing
func handleACLCreateError(resp *kmsg.CreateACLsResponse) error {
	if len(resp.Results) > 0 && resp.Results[0].ErrorCode != 0 {
		switch resp.Results[0].ErrorCode {
		case 7:
			return nil
		default:
			return fmt.Errorf("failed to create ACL: error code %v", resp.Results[0].ErrorCode)
		}
	}
	return nil
}
