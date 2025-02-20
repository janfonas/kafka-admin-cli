package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

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

func TestModifyTopic(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		config    map[string]string
		errorCode int16
		wantError bool
		errorMsg  string
	}{
		{
			name:  "success",
			topic: "test-topic",
			config: map[string]string{
				"retention.ms": "86400000",
			},
			errorCode: 0,
			wantError: false,
		},
		{
			name:  "topic not found",
			topic: "nonexistent-topic",
			config: map[string]string{
				"retention.ms": "86400000",
			},
			errorCode: 3,
			wantError: true,
			errorMsg:  "topic does not exist: nonexistent-topic",
		},
		{
			name:  "invalid topic name",
			topic: "",
			config: map[string]string{
				"retention.ms": "86400000",
			},
			errorCode: 41,
			wantError: true,
			errorMsg:  "topic name is invalid",
		},
		{
			name:  "unknown error",
			topic: "test-topic",
			config: map[string]string{
				"retention.ms": "86400000",
			},
			errorCode: 99,
			wantError: true,
			errorMsg:  "failed to modify topic config: error code 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := newMockClient(&kmsg.AlterConfigsResponse{
				Resources: []kmsg.AlterConfigsResponseResource{
					{
						ErrorCode: tt.errorCode,
					},
				},
			})

			client := NewClientWithMock(mockClient)

			err := client.ModifyTopic(context.Background(), tt.topic, tt.config)
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
