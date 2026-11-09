package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type consumerGroupFixture struct {
	description *kmsg.DescribeGroupsResponse
	committed   *kmsg.OffsetFetchResponse
	end         *kmsg.ListOffsetsResponse
	fetchErr    error
	endErr      error
}

func newConsumerGroupFixture() *consumerGroupFixture {
	assignment := kmsg.NewConsumerMemberAssignment()
	assignment.Topics = []kmsg.ConsumerMemberAssignmentTopic{{Topic: "events", Partitions: []int32{0}}}
	return &consumerGroupFixture{
		description: &kmsg.DescribeGroupsResponse{Groups: []kmsg.DescribeGroupsResponseGroup{{
			Group: "review", State: "Stable", ProtocolType: "consumer",
			Members: []kmsg.DescribeGroupsResponseGroupMember{{ClientID: "reader", MemberAssignment: assignment.AppendTo(nil)}},
		}}},
		committed: &kmsg.OffsetFetchResponse{Topics: []kmsg.OffsetFetchResponseTopic{{
			Topic: "events", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: 100}},
		}}},
		end: &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{{
			Topic: "events", Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Offset: 150}},
		}}},
	}
}

func (fixture *consumerGroupFixture) Request(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
	switch request.(type) {
	case *kmsg.DescribeGroupsRequest:
		return fixture.description, nil
	case *kmsg.OffsetFetchRequest:
		return fixture.committed, fixture.fetchErr
	case *kmsg.ListOffsetsRequest:
		return fixture.end, fixture.endErr
	default:
		return nil, fmt.Errorf("unexpected request %T", request)
	}
}

func (*consumerGroupFixture) Close() {}

func TestConsumerGroupOffsetFailures(t *testing.T) {
	for _, testcase := range []struct {
		name   string
		change func(*consumerGroupFixture)
		cause  error
		text   string
	}{
		{name: "fetch transport", change: func(fixture *consumerGroupFixture) { fixture.fetchErr = context.DeadlineExceeded }, cause: context.DeadlineExceeded, text: "events"},
		{name: "end transport", change: func(fixture *consumerGroupFixture) { fixture.endErr = context.DeadlineExceeded }, cause: context.DeadlineExceeded, text: "events"},
		{name: "propagated group error", change: func(fixture *consumerGroupFixture) { fixture.fetchErr = kerr.GroupAuthorizationFailed }, cause: kerr.GroupAuthorizationFailed, text: "review"},
		{name: "response group error", change: func(fixture *consumerGroupFixture) { fixture.committed.ErrorCode = kerr.GroupAuthorizationFailed.Code }, cause: kerr.GroupAuthorizationFailed, text: "review"},
		{name: "committed partition error", change: func(fixture *consumerGroupFixture) {
			fixture.committed.Topics[0].Partitions[0].ErrorCode = kerr.TopicAuthorizationFailed.Code
		}, cause: kerr.TopicAuthorizationFailed, text: "partition 0"},
		{name: "end partition error", change: func(fixture *consumerGroupFixture) {
			fixture.end.Topics[0].Partitions[0].ErrorCode = kerr.TopicAuthorizationFailed.Code
		}, cause: kerr.TopicAuthorizationFailed, text: "partition 0"},
		{name: "missing committed topic", change: func(fixture *consumerGroupFixture) { fixture.committed.Topics = nil }, text: "events"},
		{name: "missing end topic", change: func(fixture *consumerGroupFixture) { fixture.end.Topics = nil }, text: "events"},
		{name: "missing committed partition", change: func(fixture *consumerGroupFixture) { fixture.committed.Topics[0].Partitions = nil }, text: "partition 0"},
		{name: "missing end partition", change: func(fixture *consumerGroupFixture) { fixture.end.Topics[0].Partitions = nil }, text: "partition 0"},
		{name: "wrong committed topic", change: func(fixture *consumerGroupFixture) { fixture.committed.Topics[0].Topic = "other" }, text: "events"},
		{name: "wrong end topic", change: func(fixture *consumerGroupFixture) { fixture.end.Topics[0].Topic = "other" }, text: "events"},
		{name: "duplicate committed partition", change: func(fixture *consumerGroupFixture) {
			fixture.committed.Topics[0].Partitions = append(fixture.committed.Topics[0].Partitions, fixture.committed.Topics[0].Partitions[0])
		}, text: "duplicate"},
		{name: "duplicate end partition", change: func(fixture *consumerGroupFixture) {
			fixture.end.Topics[0].Partitions = append(fixture.end.Topics[0].Partitions, fixture.end.Topics[0].Partitions[0])
		}, text: "duplicate"},
		{name: "unknown end offset", change: func(fixture *consumerGroupFixture) { fixture.end.Topics[0].Partitions[0].Offset = -1 }, text: "end offset"},
		{name: "invalid committed offset", change: func(fixture *consumerGroupFixture) { fixture.committed.Topics[0].Partitions[0].Offset = -2 }, text: "committed offset"},
		{name: "wrong group", change: func(fixture *consumerGroupFixture) { fixture.description.Groups[0].Group = "other" }, text: "review"},
		{name: "missing group", change: func(fixture *consumerGroupFixture) { fixture.description.Groups = nil }, text: "review"},
		{name: "invalid assignment", change: func(fixture *consumerGroupFixture) {
			fixture.description.Groups[0].Members[0].MemberAssignment = []byte{1}
		}, text: "assignment"},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Errorf("unexpected panic: %v", recovered)
				}
			}()
			fixture := newConsumerGroupFixture()
			testcase.change(fixture)
			details, err := NewClientWithMock(fixture).GetConsumerGroup(context.Background(), "review")
			if err == nil || details != nil {
				t.Fatalf("expected failure without partial details, got %+v, %v", details, err)
			}
			if testcase.cause != nil && !errors.Is(err, testcase.cause) {
				t.Errorf("expected wrapped %v, got %v", testcase.cause, err)
			}
			if !strings.Contains(err.Error(), testcase.text) {
				t.Errorf("error %q lacks context %q", err, testcase.text)
			}
		})
	}
}

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
		wantErr        bool
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
			name:           "empty partition - end is zero",
			current:        0,
			end:            0,
			wantLag:        0,
			wantIsEmpty:    true,
			wantEndDisplay: "Empty",
		},
		{
			name:           "empty partition - consumer hasn't committed yet",
			current:        -1,
			end:            0,
			wantLag:        0,
			wantIsEmpty:    true,
			wantEndDisplay: "Empty",
		},
		{
			name:    "unknown end is not caught up",
			current: 400739,
			end:     -1,
			wantErr: true,
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
			fixture := newConsumerGroupFixture()
			fixture.committed.Topics[0].Partitions[0].Offset = tt.current
			fixture.end.Topics[0].Partitions[0].Offset = tt.end
			details, err := NewClientWithMock(fixture).GetConsumerGroup(context.Background(), "review")
			if tt.wantErr {
				if err == nil || details != nil {
					t.Fatalf("expected unknown offset error, got %+v, %v", details, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			offset := details.Offsets["events"][0]
			if offset.Lag != tt.wantLag {
				t.Errorf("expected lag %d, got %d", tt.wantLag, offset.Lag)
			}
			if offset.IsEmpty != tt.wantIsEmpty {
				t.Errorf("expected isEmpty %v, got %v", tt.wantIsEmpty, offset.IsEmpty)
			}
			if offset.EndDisplay != tt.wantEndDisplay {
				t.Errorf("expected endDisplay %q, got %q", tt.wantEndDisplay, offset.EndDisplay)
			}
		})
	}
}

func TestConsumerGroupNonConsumerProtocol(t *testing.T) {
	fixture := newConsumerGroupFixture()
	fixture.description.Groups[0].ProtocolType = "connect"
	fixture.description.Groups[0].Members[0].MemberAssignment = []byte{1}
	details, err := NewClientWithMock(fixture).GetConsumerGroup(context.Background(), "review")
	if err != nil {
		t.Fatal(err)
	}
	if len(details.Members) != 1 || details.Members[0].ClientID != "reader" {
		t.Fatalf("expected Connect member details, got %+v", details.Members)
	}
	if len(details.Offsets) != 0 || len(details.Members[0].Assignments) != 0 {
		t.Fatalf("non-consumer assignments must not be interpreted as consumer offsets: %+v", details)
	}
}

func TestConsumerGroupOffsetsMatchPartitionIDs(t *testing.T) {
	fixture := newConsumerGroupFixture()
	assignment := kmsg.NewConsumerMemberAssignment()
	assignment.Topics = []kmsg.ConsumerMemberAssignmentTopic{{Topic: "events", Partitions: []int32{0, 2}}}
	fixture.description.Groups[0].Members[0].MemberAssignment = assignment.AppendTo(nil)
	fixture.committed.Topics = []kmsg.OffsetFetchResponseTopic{
		{Topic: "other", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 0, Offset: 999}}},
		{Topic: "events", Partitions: []kmsg.OffsetFetchResponseTopicPartition{{Partition: 2, Offset: 40}, {Partition: 0, Offset: 100}}},
	}
	fixture.end.Topics = []kmsg.ListOffsetsResponseTopic{
		{Topic: "other", Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Offset: 1000}}},
		{Topic: "events", Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, Offset: 150}, {Partition: 2, Offset: 47}}},
	}
	details, err := NewClientWithMock(fixture).GetConsumerGroup(context.Background(), "review")
	if err != nil {
		t.Fatal(err)
	}
	for partition, want := range map[int32]PartitionOffset{
		0: {Current: 100, End: 150, Lag: 50, EndDisplay: "150"},
		2: {Current: 40, End: 47, Lag: 7, EndDisplay: "47"},
	} {
		if got := details.Offsets["events"][partition]; got != want {
			t.Errorf("partition %d: got %+v, want %+v", partition, got, want)
		}
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
