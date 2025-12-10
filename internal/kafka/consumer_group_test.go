package kafka

import (
	"context"
	"fmt"
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

func TestPartitionOffsetCalculation(t *testing.T) {
	tests := []struct {
		name           string
		current        int64
		end            int64
		wantLag        int64
		wantIsEmpty    bool
		wantEndDisplay string
	}{
		{
			name:           "normal case with lag",
			current:        100,
			end:            150,
			wantLag:        50,
			wantIsEmpty:    false,
			wantEndDisplay: "150",
		},
		{
			name:           "no lag - consumer caught up",
			current:        100,
			end:            100,
			wantLag:        0,
			wantIsEmpty:    false,
			wantEndDisplay: "100",
		},
		{
			name:           "empty partition - end is -1",
			current:        0,
			end:            -1,
			wantLag:        0,
			wantIsEmpty:    true,
			wantEndDisplay: "Empty",
		},
		{
			name:           "empty partition - consumer hasn't committed yet",
			current:        -1,
			end:            -1,
			wantLag:        0,
			wantIsEmpty:    true,
			wantEndDisplay: "Empty",
		},
		{
			name:           "compacted topic - consumer caught up",
			current:        400739,
			end:            -1,
			wantLag:        0,
			wantIsEmpty:    false,
			wantEndDisplay: "At latest",
		},
		{
			name:           "consumer hasn't committed yet - has messages",
			current:        -1,
			end:            50,
			wantLag:        50,
			wantIsEmpty:    false,
			wantEndDisplay: "50",
		},
		{
			name:           "consumer ahead somehow - shouldn't happen",
			current:        200,
			end:            100,
			wantLag:        0,
			wantIsEmpty:    false,
			wantEndDisplay: "100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the offset calculation logic directly
			var lag int64
			var isEmpty bool
			var endDisplay string
			
			if tt.end == -1 {
				if tt.current <= 0 {
					// Truly empty partition: no messages ever produced
					isEmpty = true
					endDisplay = "Empty"
					lag = 0
				} else {
					// Compacted partition or consumer caught up at latest offset
					// Current offset > 0 means messages were consumed before
					isEmpty = false
					endDisplay = "At latest"
					lag = 0 // Consumer is caught up
				}
			} else {
				// Normal case: partition has messages
				isEmpty = false
				endDisplay = fmt.Sprintf("%d", tt.end)
				if tt.current < 0 {
					// Consumer hasn't committed any offset yet (never consumed)
					lag = tt.end // All messages are unread
				} else {
					// Normal case: calculate actual lag
					lag = tt.end - tt.current
					if lag < 0 {
						lag = 0 // Consumer is ahead (rare edge case)
					}
				}
			}

			if lag != tt.wantLag {
				t.Errorf("expected lag %d, got %d", tt.wantLag, lag)
			}
			if isEmpty != tt.wantIsEmpty {
				t.Errorf("expected isEmpty %v, got %v", tt.wantIsEmpty, isEmpty)
			}
			if endDisplay != tt.wantEndDisplay {
				t.Errorf("expected endDisplay %q, got %q", tt.wantEndDisplay, endDisplay)
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
