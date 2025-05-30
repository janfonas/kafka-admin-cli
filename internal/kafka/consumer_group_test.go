package kafka

import (
	"context"
	"testing"
)

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

func TestDeleteConsumerGroup(t *testing.T) {
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
			errorMsg:  "consumer group not found: test-group",
		},
		{
			name:      "invalid group id",
			errorCode: 24,
			wantError: true,
			errorMsg:  "invalid consumer group id: test-group",
		},
		{
			name:      "group not empty",
			errorCode: 25,
			wantError: true,
			errorMsg:  "consumer group is not empty: test-group",
		},
		{
			name:      "unknown error",
			errorCode: 99,
			wantError: true,
			errorMsg:  "failed to delete consumer group: error code 99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := NewMockClientWithDeleteGroupsResponse(tt.errorCode)
			client := &Client{client: mockClient}
			err := client.DeleteConsumerGroup(context.Background(), "test-group")

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
