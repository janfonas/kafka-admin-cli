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
	Current    int64
	End        int64
	Lag        int64
	IsEmpty    bool   // Indicates if the partition has no messages
	EndDisplay string // Human-readable end offset display
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
	req := kmsg.NewPtrListGroupsRequest()
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	if resp.ErrorCode != 0 {
		return nil, requestError("list consumer groups", resp.ErrorCode, nil)
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
	descReq := kmsg.NewPtrDescribeGroupsRequest()
	descReq.Groups = []string{groupID}
	descResp, err := descReq.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer group: %w", err)
	}

	if len(descResp.Groups) == 0 {
		return nil, fmt.Errorf("group not found: %s", groupID)
	}
	if len(descResp.Groups) != 1 || descResp.Groups[0].Group != groupID {
		return nil, fmt.Errorf("invalid description response for consumer group %q", groupID)
	}

	group := descResp.Groups[0]
	if group.ErrorCode != 0 {
		return nil, requestError(fmt.Sprintf("describe consumer group %q", groupID), group.ErrorCode, group.ErrorMessage)
	}

	// Parse members and their assignments
	members := make([]ConsumerGroupMember, 0, len(group.Members))
	topicPartitions := make(map[string][]int32)

	for _, member := range group.Members {
		assignments := make(map[string][]int32)
		if group.ProtocolType == "consumer" && member.MemberAssignment != nil {
			// Parse member assignment
			var memberAssignment kmsg.ConsumerMemberAssignment
			err := memberAssignment.ReadFrom(member.MemberAssignment)
			if err != nil {
				return nil, fmt.Errorf("invalid assignment for member %q of consumer group %q: %w", member.ClientID, groupID, err)
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
		// Populate both v0-v7 fields (Group/Topics) and v8+ field (Groups)
		// so the correct one is serialized regardless of negotiated API version.
		offsetReqTopic := kmsg.NewOffsetFetchRequestTopic()
		offsetReqTopic.Topic = topic
		offsetReqTopic.Partitions = partitions

		offsetReqGroupTopic := kmsg.NewOffsetFetchRequestGroupTopic()
		offsetReqGroupTopic.Topic = topic
		offsetReqGroupTopic.Partitions = partitions

		offsetReqGroup := kmsg.NewOffsetFetchRequestGroup()
		offsetReqGroup.Group = groupID
		offsetReqGroup.Topics = []kmsg.OffsetFetchRequestGroupTopic{offsetReqGroupTopic}

		offsetReq := kmsg.NewPtrOffsetFetchRequest()
		offsetReq.Group = groupID                                         // v0-v7
		offsetReq.Topics = []kmsg.OffsetFetchRequestTopic{offsetReqTopic} // v0-v7
		offsetReq.Groups = []kmsg.OffsetFetchRequestGroup{offsetReqGroup} // v8+
		offsetResp, err := offsetReq.RequestWith(ctx, c.client)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch offsets for consumer group %q topic %q: %w", groupID, topic, err)
		}
		if offsetResp.ErrorCode != 0 {
			return nil, requestError(fmt.Sprintf("fetch offsets for consumer group %q topic %q", groupID, topic), offsetResp.ErrorCode, nil)
		}

		currentByPartition := make(map[int32]int64, len(partitions))
		for _, responseTopic := range offsetResp.Topics {
			if responseTopic.Topic != topic {
				continue
			}
			for _, responsePartition := range responseTopic.Partitions {
				operation := fmt.Sprintf("fetch offset for consumer group %q topic %q partition %d", groupID, topic, responsePartition.Partition)
				if responsePartition.ErrorCode != 0 {
					return nil, requestError(operation, responsePartition.ErrorCode, nil)
				}
				if _, exists := currentByPartition[responsePartition.Partition]; exists {
					return nil, fmt.Errorf("failed to %s: duplicate partition", operation)
				}
				if responsePartition.Offset < -1 {
					return nil, fmt.Errorf("failed to %s: invalid committed offset %d", operation, responsePartition.Offset)
				}
				currentByPartition[responsePartition.Partition] = responsePartition.Offset
			}
		}

		// Get end offsets
		endOffsetReqParts := make([]kmsg.ListOffsetsRequestTopicPartition, len(partitions))
		for i, p := range partitions {
			part := kmsg.NewListOffsetsRequestTopicPartition()
			part.Partition = p
			part.Timestamp = -1 // Latest offset
			endOffsetReqParts[i] = part
		}

		endOffsetReqTopic := kmsg.NewListOffsetsRequestTopic()
		endOffsetReqTopic.Topic = topic
		endOffsetReqTopic.Partitions = endOffsetReqParts

		endOffsetReq := kmsg.NewPtrListOffsetsRequest()
		endOffsetReq.Topics = []kmsg.ListOffsetsRequestTopic{endOffsetReqTopic}
		endOffsetResp, err := endOffsetReq.RequestWith(ctx, c.client)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch end offsets for consumer group %q topic %q: %w", groupID, topic, err)
		}

		endByPartition := make(map[int32]int64, len(partitions))
		for _, responseTopic := range endOffsetResp.Topics {
			if responseTopic.Topic != topic {
				continue
			}
			for _, responsePartition := range responseTopic.Partitions {
				operation := fmt.Sprintf("fetch end offset for consumer group %q topic %q partition %d", groupID, topic, responsePartition.Partition)
				if responsePartition.ErrorCode != 0 {
					return nil, requestError(operation, responsePartition.ErrorCode, nil)
				}
				if _, exists := endByPartition[responsePartition.Partition]; exists {
					return nil, fmt.Errorf("failed to %s: duplicate partition", operation)
				}
				if responsePartition.Offset < 0 {
					return nil, fmt.Errorf("failed to %s: unavailable end offset %d", operation, responsePartition.Offset)
				}
				endByPartition[responsePartition.Partition] = responsePartition.Offset
			}
		}

		offsets[topic] = make(map[int32]PartitionOffset)
		for _, partition := range partitions {
			current, ok := currentByPartition[partition]
			if !ok {
				return nil, fmt.Errorf("missing committed offset for consumer group %q topic %q partition %d", groupID, topic, partition)
			}
			end, ok := endByPartition[partition]
			if !ok {
				return nil, fmt.Errorf("missing end offset for consumer group %q topic %q partition %d", groupID, topic, partition)
			}

			lag := end
			if current >= 0 {
				lag = max(end-current, 0)
			}
			isEmpty := end == 0
			endDisplay := fmt.Sprintf("%d", end)
			if isEmpty {
				endDisplay = "Empty"
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
	offsetPartition := kmsg.NewOffsetCommitRequestTopicPartition()
	offsetPartition.Partition = partition
	offsetPartition.Offset = offset

	offsetTopic := kmsg.NewOffsetCommitRequestTopic()
	offsetTopic.Topic = topic
	offsetTopic.Partitions = []kmsg.OffsetCommitRequestTopicPartition{offsetPartition}

	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = groupID
	req.Topics = []kmsg.OffsetCommitRequestTopic{offsetTopic}

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to commit offset: %w", err)
	}

	if len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 {
		errorCode := resp.Topics[0].Partitions[0].ErrorCode
		if errorCode != 0 {
			return requestError(fmt.Sprintf("commit offset for consumer group %q", groupID), errorCode, nil)
		}
	}

	return nil
}

// DeleteConsumerGroup Deletes a consumer group with the specified ID.
// Returns an error if the group doesn't exist or if the operation fails.
func (c *Client) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	req := kmsg.NewPtrDeleteGroupsRequest()
	req.Groups = []string{groupID}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete consumer group: %w", err)
	}

	if len(resp.Groups) > 0 && resp.Groups[0].ErrorCode != 0 {
		return requestError(fmt.Sprintf("delete consumer group %q", groupID), resp.Groups[0].ErrorCode, nil)
	}
	return nil
}
