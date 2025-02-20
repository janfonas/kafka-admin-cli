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
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Client struct {
	client      *kgo.Client
	adminClient *kadm.Client
}

func NewClient(brokers []string, username, password, caCertPath, saslMechanism string, insecure bool) (*Client, error) {
	var saslOption kgo.Opt
	if err := validateSASLMechanism(saslMechanism); err != nil {
		return nil, err
	}

	auth, err := configureSASL(username, password, saslMechanism)
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(saslMechanism) {
	case "SCRAM-SHA-512":
		saslOption = kgo.SASL(auth.(sasl.Mechanism))
	case "PLAIN":
		saslOption = kgo.SASL(auth.(plain.Auth).AsMechanism())
	}

	seeds := make([]string, len(brokers))
	for i, broker := range brokers {
		u, err := parseURL(broker)
		if err != nil {
			return nil, fmt.Errorf("invalid broker URL %q: %w", broker, err)
		}
		seeds[i] = u
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
			RootCAs:            caCertPool,
			InsecureSkipVerify: insecure,
		}
	} else {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: insecure,
		}
	}

	dialer := func(ctx context.Context, network, host string) (net.Conn, error) {
		return (&tls.Dialer{Config: tlsConfig}).DialContext(ctx, network, host)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		saslOption,
		kgo.Dialer(dialer),
		kgo.RequestTimeoutOverhead(time.Second * 5),
		kgo.MetadataMinAge(time.Second * 5),
		kgo.MetadataMaxAge(time.Second * 10),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	return &Client{
		client:      client,
		adminClient: kadm.NewClient(client),
	}, nil
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
	return handleTopicCreateError(resp, topic, partitions, replicationFactor)
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

type TopicDetails struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Config            map[string]string
}

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

func (c *Client) ModifyAcl(ctx context.Context, resourceType, resourceName, principal, host, operation, permission string, newPermission string) error {
	// First delete the existing ACL
	err := c.DeleteAcl(ctx, resourceType, resourceName, principal, host, operation, permission)
	if err != nil {
		return fmt.Errorf("failed to delete existing ACL: %w", err)
	}

	// Then create the new ACL with updated permission
	err = c.CreateAcl(ctx, resourceType, resourceName, principal, host, operation, newPermission)
	if err != nil {
		return fmt.Errorf("failed to create new ACL: %w", err)
	}

	return nil
}

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
	return handleACLCreateError(resp)
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
		switch resp.Results[0].ErrorCode {
		case 7:
			// Error code 7 during deletion seems to be returned when the operation is successful
			// but the metadata is still being updated
			return nil
		default:
			return fmt.Errorf("failed to delete ACL: error code %v", resp.Results[0].ErrorCode)
		}
	}
	return nil
}

func (c *Client) GetAcl(ctx context.Context, resourceType, resourceName, principal string) ([]kmsg.DescribeACLsResponseResource, error) {
	resourceTypeInt, err := strconv.Atoi(resourceType)
	if err != nil {
		return nil, fmt.Errorf("invalid resource type: %w", err)
	}

	req := &kmsg.DescribeACLsRequest{
		ResourceType: kmsg.ACLResourceType(resourceTypeInt),
		ResourceName: &resourceName,
		Principal:    &principal,
	}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to get ACL: %w", err)
	}
	if resp.ErrorCode != 0 {
		return nil, fmt.Errorf("failed to get ACL: %v", resp.ErrorCode)
	}
	if len(resp.Resources) == 0 {
		return nil, fmt.Errorf("no ACLs found for resource type %s, name %s, and principal %s", resourceType, resourceName, principal)
	}
	return resp.Resources, nil
}

type ConsumerGroupMember struct {
	ClientID    string
	ClientHost  string
	Assignments map[string][]int32 // topic -> partitions
}

type PartitionOffset struct {
	Current int64
	End     int64
	Lag     int64
}

type ConsumerGroupDetails struct {
	State   string
	Members []ConsumerGroupMember
	Offsets map[string]map[int32]PartitionOffset // topic -> partition -> offset
}

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
			offsets[topic][partition] = PartitionOffset{
				Current: current,
				End:     end,
				Lag:     end - current,
			}
		}
	}

	return &ConsumerGroupDetails{
		State:   group.State,
		Members: members,
		Offsets: offsets,
	}, nil
}

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

func (c *Client) ListAcls(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req := &kmsg.DescribeACLsRequest{}
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to list ACLs: %w", err)
	}

	if resp.ErrorCode != 0 {
		return nil, fmt.Errorf("failed to list ACLs: error code %v", resp.ErrorCode)
	}

	principalSet := make(map[string]struct{})
	for _, resource := range resp.Resources {
		for _, acl := range resource.ACLs {
			if strings.HasPrefix(acl.Principal, "User:") {
				principal := strings.TrimPrefix(acl.Principal, "User:")
				principalSet[principal] = struct{}{}
			}
		}
	}

	var principals []string
	for principal := range principalSet {
		principals = append(principals, principal)
	}

	return principals, nil
}

// Helper functions

func parseURL(broker string) (string, error) {
	if broker == "" {
		return "", fmt.Errorf("empty broker address")
	}

	u, err := url.Parse("//" + broker)
	if err != nil {
		return "", err
	}

	hostname := u.Hostname()
	if strings.Contains(hostname, ":") && !strings.HasPrefix(hostname, "[") {
		hostname = "[" + hostname + "]"
	}

	if u.Port() == "" {
		return fmt.Sprintf("%s:9092", hostname), nil
	}
	return fmt.Sprintf("%s:%s", hostname, u.Port()), nil
}

func validateSASLMechanism(mechanism string) error {
	switch mechanism {
	case "SCRAM-SHA-512", "PLAIN":
		return nil
	default:
		return fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
}

func configureSASL(username, password, mechanism string) (interface{}, error) {
	if username == "" {
		return nil, fmt.Errorf("username is required")
	}
	if password == "" {
		return nil, fmt.Errorf("password is required")
	}

	switch mechanism {
	case "SCRAM-SHA-512":
		return scram.Sha512(func(ctx context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: username,
				Pass: password,
			}, nil
		}), nil
	case "PLAIN":
		return plain.Auth{
			User: username,
			Pass: password,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
}

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

func handleACLCreateError(resp *kmsg.CreateACLsResponse) error {
	if len(resp.Results) > 0 && resp.Results[0].ErrorCode != 0 {
		switch resp.Results[0].ErrorCode {
		case 7:
			return nil
		case 87:
			return fmt.Errorf("invalid resource type or name")
		case 88:
			return fmt.Errorf("invalid principal format")
		default:
			return fmt.Errorf("failed to create ACL: error code %v", resp.Results[0].ErrorCode)
		}
	}
	return nil
}

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
