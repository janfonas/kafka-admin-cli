package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// kafkaClient defines the interface for Kafka client operations.
// Implements the kmsg.Requestor interface for making Kafka protocol requests
// and provides a Close method for cleanup.
type kafkaClient interface {
	kmsg.Requestor
	Close()
}

// Client Represents a Kafka client with both basic client functionality
// and administrative capabilities through the admin client.
type Client struct {
	client      kafkaClient // Using interface for testing
	adminClient *kadm.Client
	hooks       *brokerConnect // Store hooks for real client
}

// brokerConnect is a callback that is invoked when a connection to a broker is established.
// It is used to handle connection errors and close the client if necessary.
type brokerConnect struct {
	once    sync.Once
	rClient *kgo.Client
}

// OnBrokerConnect is invoked when a connection to a broker is established.
// It is used to handle connection errors and close the client if necessary.
func (bc *brokerConnect) OnBrokerConnect(meta kgo.BrokerMetadata, dialDur time.Duration, conn net.Conn, err error) {
	bc.once.Do(func() {
		if err != nil {
			fmt.Printf("Error establishing connection to broker %s: %v\n", meta.Host, err)
			if bc.rClient != nil {
				bc.rClient.Close()
			}
		} else {
			fmt.Printf("Successfully connected to broker %s (took %v)\n", meta.Host, dialDur)
		}
	})
}

// NewClient Creates a new Kafka client with the specified configuration.
// Supports SASL authentication (SCRAM-SHA-512 and PLAIN) and TLS encryption.
// The client is configured with appropriate timeouts and metadata refresh intervals.
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

	// Create the hook first
	bc := &brokerConnect{}

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		saslOption,
		kgo.Dialer(dialer),
		kgo.RequestTimeoutOverhead(time.Second * 5),
		kgo.MetadataMinAge(time.Second * 5),
		kgo.MetadataMaxAge(time.Second * 10),
		kgo.WithHooks(bc),
	}

	// Create the client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Set the client reference in the hook
	bc.rClient = client

	// Create the admin client with the concrete kgo.Client
	adminClient := kadm.NewClient(client)

	return &Client{
		client:      client,
		adminClient: adminClient,
		hooks:       bc,
	}, nil
}

// NewClientWithMock Creates a new Client with a mock kafka client.
// This is used for testing purposes.
func NewClientWithMock(mockClient kafkaClient) *Client {
	// For testing, we don't need a real admin client since we're mocking the responses
	return &Client{
		client: mockClient,
	}
}

// Close Closes the Kafka client connection and cleans up resources.
func (c *Client) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// CheckAPISupport checks whether the broker supports a specific Kafka API key.
// Returns (supported, maxVersion, error). This is useful for diagnosing when
// specific features (like ACLs) are not available on the cluster.
func (c *Client) CheckAPISupport(ctx context.Context, apiKey int16) (bool, int16, error) {
	req := kmsg.NewPtrApiVersionsRequest()
	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return false, 0, fmt.Errorf("failed to query API versions: %w", err)
	}
	if resp.ErrorCode != 0 {
		return false, 0, fmt.Errorf("API versions request returned error code %d", resp.ErrorCode)
	}
	for _, api := range resp.ApiKeys {
		if api.ApiKey == apiKey {
			return true, api.MaxVersion, nil
		}
	}
	return false, 0, nil
}
