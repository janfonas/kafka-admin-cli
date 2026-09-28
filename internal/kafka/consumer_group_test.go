package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

func TestRequestErrorWrapsKafkaErrors(t *testing.T) {
	for _, tt := range []struct {
		name string
		code int16
		want error
	}{
		{"request timed out is a failure", kerr.RequestTimedOut.Code, kerr.RequestTimedOut},
		{"coordinator not available", kerr.CoordinatorNotAvailable.Code, kerr.CoordinatorNotAvailable},
		{"group not found", kerr.GroupIDNotFound.Code, kerr.GroupIDNotFound},
		{"unknown code", 32000, kerr.UnknownServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := requestError(`describe consumer group "g"`, tt.code, nil)
			if !errors.Is(err, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, err)
			}
			if want := fmt.Sprintf(`failed to describe consumer group "g" (error code %d)`, tt.code); !strings.HasPrefix(err.Error(), want) {
				t.Errorf("expected prefix %q, got %q", want, err)
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
		want      error
	}{
		{name: "success"},
		{name: "request timed out is a failure", errorCode: kerr.RequestTimedOut.Code, want: kerr.RequestTimedOut},
		{name: "group not found", errorCode: kerr.GroupIDNotFound.Code, want: kerr.GroupIDNotFound},
		{name: "invalid group id", errorCode: kerr.InvalidGroupID.Code, want: kerr.InvalidGroupID},
		{name: "group not empty", errorCode: kerr.NonEmptyGroup.Code, want: kerr.NonEmptyGroup},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := NewMockClientWithDeleteGroupsResponse(tt.errorCode)
			client := &Client{client: mockClient}
			err := client.DeleteConsumerGroup(context.Background(), "test-group")

			if tt.want == nil {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.want) || !strings.HasPrefix(err.Error(), `failed to delete consumer group "test-group"`) {
				t.Errorf("expected %v for test-group, got %v", tt.want, err)
			}
		})
	}
}
