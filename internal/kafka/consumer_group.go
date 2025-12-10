package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// ConsumerGroupMember Contains information about a member of a consumer group,
// including their client ID, host, and partition assignments.
type ConsumerGroupMember struct {
	ClientID    string
	ClientHost  string
	Assignments map[string][]int32 // topic -> partitions
}

// PartitionOffset Contains offset information for a partition,
// including current position, end offset, and the lag.
type PartitionOffset struct {
	Current     int64
	End         int64
	Lag         int64
	IsEmpty     bool   // Indicates if the partition has no messages
	EndDisplay  string // Human-readable end offset display
}

// ConsumerGroupDetails Contains detailed information about a consumer group,
// including its state, members, and offset information for all partitions.
type ConsumerGroupDetails struct {
	State   string
	Members []ConsumerGroupMember
	Offsets map[string]map[int32]PartitionOffset // topic -> partition -> offset
}

// ListConsumerGroups Returns a list of all consumer group IDs in the cluster.
func (c *Client) ListConsumerGroups(ctx context.Context) ([]string, error) {
	req := &kmsg.ListGroupsRequest{}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	var groups []string
	for _, group := range resp.Groups {
		groups = append(groups, group.Group)
	}
	return groups, nil
}

// GetConsumerGroup Retrieves detailed information about a specific consumer group.
// Returns information about the group's state, members, and their partition assignments,
// as well as current offset positions and lag for each partition.
func (c *Client) GetConsumerGroup(ctx context.Context, groupID string) (*ConsumerGroupDetails, error) {
	// Get group description
	descReq := &kmsg.DescribeGroupsRequest{
		Groups: []string{groupID},
	}
	descResp, err := descReq.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer group: %w", err)
	}

	if len(descResp.Groups) == 0 {
		return nil, fmt.Errorf("group not found: %s", groupID)
	}

	group := descResp.Groups[0]
	if group.ErrorCode != 0 {
		return nil, handleConsumerGroupError(group.ErrorCode)
	}

	// Parse members and their assignments
	members := make([]ConsumerGroupMember, 0, len(group.Members))
	topicPartitions := make(map[string][]int32)

	for _, member := range group.Members {
		assignments := make(map[string][]int32)
		if member.MemberAssignment != nil {
			// Parse member assignment
			var memberAssignment kmsg.ConsumerMemberAssignment
			err := memberAssignment.ReadFrom(member.MemberAssignment)
			if err != nil {
				continue
			}

			for _, topic := range memberAssignment.Topics {
				assignments[topic.Topic] = topic.Partitions
				topicPartitions[topic.Topic] = append(topicPartitions[topic.Topic], topic.Partitions...)
			}
		}

		members = append(members, ConsumerGroupMember{
			ClientID:    member.ClientID,
			ClientHost:  member.ClientHost,
			Assignments: assignments,
		})
	}

	// Get offsets for all topic partitions
	offsets := make(map[string]map[int32]PartitionOffset)
	for topic, partitions := range topicPartitions {
		offsetReq := &kmsg.OffsetFetchRequest{
			Group: groupID,
			Topics: []kmsg.OffsetFetchRequestTopic{{
				Topic:      topic,
				Partitions: partitions,
			}},
		}
		offsetResp, err := offsetReq.RequestWith(ctx, c.client)
		if err != nil {
			continue
		}

		// Get end offsets
		endOffsetReq := &kmsg.ListOffsetsRequest{
			Topics: []kmsg.ListOffsetsRequestTopic{{
				Topic: topic,
				Partitions: func() []kmsg.ListOffsetsRequestTopicPartition {
					parts := make([]kmsg.ListOffsetsRequestTopicPartition, len(partitions))
					for i, p := range partitions {
						parts[i] = kmsg.ListOffsetsRequestTopicPartition{
							Partition: p,
							Timestamp: -1, // Latest offset
						}
					}
					return parts
				}(),
			}},
		}
		endOffsetResp, err := endOffsetReq.RequestWith(ctx, c.client)
		if err != nil {
			continue
		}

		offsets[topic] = make(map[int32]PartitionOffset)
		for i, partition := range partitions {
			current := offsetResp.Topics[0].Partitions[i].Offset
			end := endOffsetResp.Topics[0].Partitions[i].Offset
			
			var lag int64
			var isEmpty bool
			var endDisplay string
			
			if end == -1 {
				if current <= 0 {
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
				endDisplay = fmt.Sprintf("%d", end)
				if current < 0 {
					// Consumer hasn't committed any offset yet (never consumed)
					lag = end // All messages are unread
				} else {
					// Normal case: calculate actual lag
					lag = end - current
					if lag < 0 {
						lag = 0 // Consumer is ahead (rare edge case)
					}
				}
			}
			
			offsets[topic][partition] = PartitionOffset{
				Current:    current,
				End:        end,
				Lag:        lag,
				IsEmpty:    isEmpty,
				EndDisplay: endDisplay,
			}
		}
	}

	return &ConsumerGroupDetails{
		State:   group.State,
		Members: members,
		Offsets: offsets,
	}, nil
}

// SetConsumerGroupOffsets Updates the committed offset for a specific partition
// in a consumer group. This can be used to reset a consumer group's position
// or to skip over problematic messages.
func (c *Client) SetConsumerGroupOffsets(ctx context.Context, groupID, topic string, partition int32, offset int64) error {
	req := &kmsg.OffsetCommitRequest{
		Group: groupID,
		Topics: []kmsg.OffsetCommitRequestTopic{{
			Topic: topic,
			Partitions: []kmsg.OffsetCommitRequestTopicPartition{{
				Partition: partition,
				Offset:    offset,
			}},
		}},
	}

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to commit offset: %w", err)
	}

	if len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 {
		errorCode := resp.Topics[0].Partitions[0].ErrorCode
		if errorCode != 0 {
			return handleConsumerGroupError(errorCode)
		}
	}

	return nil
}

// DeleteConsumerGroup Deletes a consumer group with the specified ID.
// Returns an error if the group doesn't exist or if the operation fails.
func (c *Client) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	req := &kmsg.DeleteGroupsRequest{
		Groups: []string{groupID},
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete consumer group: %w", err)
	}

	if len(resp.Groups) > 0 && resp.Groups[0].ErrorCode != 0 {
		switch resp.Groups[0].ErrorCode {
		case 7:
			// Error code 7 during deletion usually means the operation was successful
			// but the metadata is still being updated
			return nil
		case 15:
			return fmt.Errorf("consumer group not found: %s", groupID)
		case 24:
			return fmt.Errorf("invalid consumer group id: %s", groupID)
		case 25:
			return fmt.Errorf("consumer group is not empty: %s", groupID)
		default:
			return fmt.Errorf("failed to delete consumer group: error code %v", resp.Groups[0].ErrorCode)
		}
	}
	return nil
}

// handleConsumerGroupError Processes error codes from consumer group operations
// and returns appropriate error messages.
func handleConsumerGroupError(errorCode int16) error {
	if errorCode != 0 {
		switch errorCode {
		case 7:
			return nil
		case 15:
			return fmt.Errorf("consumer group not found")
		case 24:
			return fmt.Errorf("invalid consumer group id")
		default:
			return fmt.Errorf("failed to process consumer group request: error code %v", errorCode)
		}
	}
	return nil
}
