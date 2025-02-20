package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TopicDetails Contains metadata about a Kafka topic including its name,
// number of partitions, replication factor, and configuration settings.
type TopicDetails struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]string
}

// CreateTopic Creates a new Kafka topic with the specified name, number of partitions,
// and replication factor. Returns an error if the topic already exists or if the
// parameters are invalid.
func (c *Client) CreateTopic(ctx context.Context, topic string, partitions int, replicationFactor int) error {
	req := &kmsg.CreateTopicsRequest{
		Topics: []kmsg.CreateTopicsRequestTopic{
			{
				Topic:             topic,
				NumPartitions:     int32(partitions),
				ReplicationFactor: int16(replicationFactor),
			},
		},
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return handleTopicCreateError(resp, topic, partitions, replicationFactor)
}

// DeleteTopic Deletes a Kafka topic with the specified name.
// Returns an error if the topic doesn't exist or if the name is invalid.
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	topicPtr := topic
	req := &kmsg.DeleteTopicsRequest{
		Topics: []kmsg.DeleteTopicsRequestTopic{
			{
				Topic: &topicPtr,
			},
		},
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}
	if len(resp.Topics) > 0 && resp.Topics[0].ErrorCode != 0 {
		switch resp.Topics[0].ErrorCode {
		case 3:
			return fmt.Errorf("topic does not exist: %s", topic)
		case 7:
			// Error code 7 during deletion usually means the topic is already being deleted
			// or the operation was successful but the metadata is still being updated
			return nil
		case 41:
			return fmt.Errorf("topic name is invalid")
		default:
			return fmt.Errorf("failed to delete topic: error code %v", resp.Topics[0].ErrorCode)
		}
	}
	return nil
}

// ModifyTopic Updates the configuration of an existing Kafka topic.
// The config parameter is a map of configuration keys and their new values.
func (c *Client) ModifyTopic(ctx context.Context, topic string, config map[string]string) error {
	req := &kmsg.AlterConfigsRequest{
		Resources: []kmsg.AlterConfigsRequestResource{
			{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: topic,
				Configs: func() []kmsg.AlterConfigsRequestResourceConfig {
					configs := make([]kmsg.AlterConfigsRequestResourceConfig, 0, len(config))
					for key, value := range config {
						configs = append(configs, kmsg.AlterConfigsRequestResourceConfig{
							Name:  key,
							Value: &value,
						})
					}
					return configs
				}(),
			},
		},
	}

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to modify topic config: %w", err)
	}

	if len(resp.Resources) > 0 && resp.Resources[0].ErrorCode != 0 {
		switch resp.Resources[0].ErrorCode {
		case 3:
			return fmt.Errorf("topic does not exist: %s", topic)
		case 41:
			return fmt.Errorf("topic name is invalid")
		default:
			return fmt.Errorf("failed to modify topic config: error code %v", resp.Resources[0].ErrorCode)
		}
	}

	return nil
}

// GetTopic Retrieves detailed information about a specific Kafka topic.
// Returns a TopicDetails struct containing the topic's metadata and configuration.
func (c *Client) GetTopic(ctx context.Context, topic string) (*TopicDetails, error) {
	req := &kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{
			{
				Topic: &topic,
			},
		},
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}

	if len(resp.Topics) == 0 {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	if resp.Topics[0].ErrorCode != 0 {
		switch resp.Topics[0].ErrorCode {
		case 3:
			return nil, fmt.Errorf("topic does not exist: %s", topic)
		default:
			return nil, fmt.Errorf("failed to get topic metadata: error code %v", resp.Topics[0].ErrorCode)
		}
	}

	// Get topic configuration
	configReq := &kmsg.DescribeConfigsRequest{
		Resources: []kmsg.DescribeConfigsRequestResource{
			{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: topic,
			},
		},
	}
	configResp, err := configReq.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic config: %w", err)
	}

	config := make(map[string]string)
	if len(configResp.Resources) > 0 {
		for _, entry := range configResp.Resources[0].Configs {
			if !entry.IsDefault {
				if entry.Value != nil {
					config[entry.Name] = *entry.Value
				}
			}
		}
	}

	details := &TopicDetails{
		Name:              topic,
		Partitions:        int32(len(resp.Topics[0].Partitions)),
		ReplicationFactor: int16(len(resp.Topics[0].Partitions[0].Replicas)),
		Config:            config,
	}

	return details, nil
}

// ListTopics Returns a list of all topic names in the Kafka cluster.
func (c *Client) ListTopics(ctx context.Context) ([]string, error) {
	req := &kmsg.MetadataRequest{}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}
	topics := make([]string, 0, len(resp.Topics))
	for _, topic := range resp.Topics {
		if topic.Topic != nil {
			topics = append(topics, *topic.Topic)
		}
	}
	return topics, nil
}

// handleTopicCreateError Processes error codes from topic creation requests
// and returns appropriate error messages.
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
