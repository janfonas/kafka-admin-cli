package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Client struct {
	client *kgo.Client
}

func NewClient(brokers []string, username, password, caCertPath string) (*Client, error) {
	auth := scram.Auth{
		User: username,
		Pass: password,
	}
	mechanism := func(ctx context.Context) (scram.Auth, error) {
		return auth, nil
	}

	seeds := make([]string, len(brokers))
	for i, broker := range brokers {
		// Parse the broker URL to handle ports correctly
		u, err := url.Parse("//" + broker) // Add scheme to make it parseable
		if err != nil {
			return nil, fmt.Errorf("invalid broker URL %q: %w", broker, err)
		}

		// If no port is specified, use default
		if u.Port() == "" {
			seeds[i] = fmt.Sprintf("%s:9092", u.Hostname())
		} else {
			seeds[i] = fmt.Sprintf("%s:%s", u.Hostname(), u.Port())
		}
	}

	var tlsConfig *tls.Config
	if caCertPath != "" {
		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig = &tls.Config{
			RootCAs: caCertPool,
		}
	} else {
		tlsConfig = &tls.Config{}
	}

	dialer := func(ctx context.Context, network, host string) (net.Conn, error) {
		return (&tls.Dialer{Config: tlsConfig}).DialContext(ctx, network, host)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.SASL(scram.Sha512(mechanism)),
		kgo.Dialer(dialer),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	return &Client{client: client}, nil
}

func (c *Client) Close() {
	c.client.Close()
}

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
	if len(resp.Topics) > 0 && resp.Topics[0].ErrorCode != 0 {
		switch resp.Topics[0].ErrorCode {
		case 7:
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

func (c *Client) CreateAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string) error {
	resourceTypeInt, err := strconv.Atoi(resourceType)
	if err != nil {
		return fmt.Errorf("invalid resource type: %w", err)
	}
	operationInt, err := strconv.Atoi(operation)
	if err != nil {
		return fmt.Errorf("invalid operation: %w", err)
	}
	permissionInt, err := strconv.Atoi(permission)
	if err != nil {
		return fmt.Errorf("invalid permission: %w", err)
	}

	req := &kmsg.CreateACLsRequest{
		Creations: []kmsg.CreateACLsRequestCreation{
			{
				ResourceType:   kmsg.ACLResourceType(resourceTypeInt),
				ResourceName:   resourceName,
				Principal:      principal,
				Host:           host,
				Operation:      kmsg.ACLOperation(operationInt),
				PermissionType: kmsg.ACLPermissionType(permissionInt),
			},
		},
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to create ACL: %w", err)
	}
	if len(resp.Results) > 0 && resp.Results[0].ErrorCode != 0 {
		return fmt.Errorf("failed to create ACL: %v", resp.Results[0].ErrorCode)
	}
	return nil
}

func (c *Client) DeleteAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string) error {
	resourceTypeInt, err := strconv.Atoi(resourceType)
	if err != nil {
		return fmt.Errorf("invalid resource type: %w", err)
	}
	operationInt, err := strconv.Atoi(operation)
	if err != nil {
		return fmt.Errorf("invalid operation: %w", err)
	}
	permissionInt, err := strconv.Atoi(permission)
	if err != nil {
		return fmt.Errorf("invalid permission: %w", err)
	}

	req := &kmsg.DeleteACLsRequest{
		Filters: []kmsg.DeleteACLsRequestFilter{
			{
				ResourceType:   kmsg.ACLResourceType(resourceTypeInt),
				ResourceName:   &resourceName,
				Principal:      &principal,
				Host:           &host,
				Operation:      kmsg.ACLOperation(operationInt),
				PermissionType: kmsg.ACLPermissionType(permissionInt),
			},
		},
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return fmt.Errorf("failed to delete ACL: %w", err)
	}
	if len(resp.Results) > 0 && resp.Results[0].ErrorCode != 0 {
		return fmt.Errorf("failed to delete ACL: %v", resp.Results[0].ErrorCode)
	}
	return nil
}

func (c *Client) ListAcls(ctx context.Context) ([]kmsg.DescribeACLsResponseResource, error) {
	req := &kmsg.DescribeACLsRequest{}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACLs: %w", err)
	}
	if resp.ErrorCode != 0 {
		return nil, fmt.Errorf("failed to list ACLs: %v", resp.ErrorCode)
	}
	return resp.Resources, nil
}
