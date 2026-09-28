package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestTopicErrorHandling(t *testing.T) {
	tests := []struct {
		name string
		code int16
		want error
	}{
		{name: "success"},
		{name: "request timed out is a failure", code: kerr.RequestTimedOut.Code, want: kerr.RequestTimedOut},
		{name: "topic exists", code: kerr.TopicAlreadyExists.Code, want: kerr.TopicAlreadyExists},
		{name: "invalid partitions", code: kerr.InvalidPartitions.Code, want: kerr.InvalidPartitions},
		{name: "invalid replication", code: kerr.InvalidReplicationFactor.Code, want: kerr.InvalidReplicationFactor},
		{name: "invalid name", code: kerr.InvalidTopicException.Code, want: kerr.InvalidTopicException},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &kmsg.CreateTopicsResponse{
				Topics: []kmsg.CreateTopicsResponseTopic{{ErrorCode: tt.code}},
			}

			err := handleTopicCreateError(resp, "test-topic", 1, 1)
			if tt.want == nil {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.want) || !strings.HasPrefix(err.Error(), `failed to create topic "test-topic" with 1 partitions and replication factor 1`) {
				t.Errorf("expected %v with topic context, got %v", tt.want, err)
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
			errorMsg:  `failed to modify topic "nonexistent-topic" config (error code 3)`,
		},
		{
			name:  "invalid topic name",
			topic: "",
			config: map[string]string{
				"retention.ms": "86400000",
			},
			errorCode: 17,
			wantError: true,
			errorMsg:  `failed to modify topic "" config (error code 17)`,
		},
		{
			name:  "unknown error",
			topic: "test-topic",
			config: map[string]string{
				"retention.ms": "86400000",
			},
			errorCode: 99,
			wantError: true,
			errorMsg:  `failed to modify topic "test-topic" config (error code 99)`,
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
					t.Fatal("expected error, got nil")
				}
				if !strings.HasPrefix(err.Error(), tt.errorMsg) || !errors.Is(err, kerr.ErrorForCode(tt.errorCode)) {
					t.Errorf("expected error prefix %q wrapping code %d, got %q", tt.errorMsg, tt.errorCode, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}
