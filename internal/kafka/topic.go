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
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topic
	reqTopic.NumPartitions = int32(partitions)
	reqTopic.ReplicationFactor = int16(replicationFactor)

	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{reqTopic}

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return handleTopicCreateError(resp, topic, partitions, replicationFactor)
}

// DeleteTopic Deletes a Kafka topic with the specified name.
// Returns an error if the topic doesn't exist or if the name is invalid.
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	// Populate both TopicNames (v0-v5) and Topics (v6+) so the correct field
	// is serialized regardless of the API version negotiated with the broker.
	reqTopic := kmsg.NewDeleteTopicsRequestTopic()
	reqTopic.Topic = &topic

	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{topic}                       // used when broker negotiates v0-v5
	req.Topics = []kmsg.DeleteTopicsRequestTopic{reqTopic} // used when broker negotiates v6+

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}
	// Response field is always resp.Topics regardless of request version
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
	configs := make([]kmsg.AlterConfigsRequestResourceConfig, 0, len(config))
	for key, value := range config {
		c := kmsg.NewAlterConfigsRequestResourceConfig()
		c.Name = key
		c.Value = &value
		configs = append(configs, c)
	}

	resource := kmsg.NewAlterConfigsRequestResource()
	resource.ResourceType = kmsg.ConfigResourceTypeTopic
	resource.ResourceName = topic
	resource.Configs = configs

	req := kmsg.NewPtrAlterConfigsRequest()
	req.Resources = []kmsg.AlterConfigsRequestResource{resource}

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
	reqTopic := kmsg.NewMetadataRequestTopic()
	reqTopic.Topic = &topic

	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{reqTopic}
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
	configResource := kmsg.NewDescribeConfigsRequestResource()
	configResource.ResourceType = kmsg.ConfigResourceTypeTopic
	configResource.ResourceName = topic

	configReq := kmsg.NewPtrDescribeConfigsRequest()
	configReq.Resources = []kmsg.DescribeConfigsRequestResource{configResource}
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
	req := kmsg.NewPtrMetadataRequest()
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
