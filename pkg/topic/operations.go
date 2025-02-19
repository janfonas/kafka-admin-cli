package topic

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type TopicOperations struct {
	client *kgo.Client
}

func NewTopicOperations(client *kgo.Client) *TopicOperations {
	return &TopicOperations{client: client}
}

func (t *TopicOperations) CreateTopic(ctx context.Context, topic string, partitions int, replicationFactor int) error {
	_, err := t.client.CreateTopics(ctx, 1, 1, map[string]kgo.TopicConfig{
		topic: {
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return nil
}

func (t *TopicOperations) DeleteTopic(ctx context.Context, topic string) error {
	_, err := t.client.DeleteTopics(ctx, []string{topic})
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}
	return nil
}

func (t *TopicOperations) ListTopics(ctx context.Context) ([]string, error) {
	topics, err := t.client.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}
	return topics, nil
}
