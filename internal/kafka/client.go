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
	client      kafkaClient
	adminClient *kadm.Client
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
			fmt.Println("Error establishing connection", "error", err)
			bc.rClient.Close()
		} else {
			fmt.Println("Connection established", "broker", meta.Host)
		}
	})
}

// NewClient Creates a new Kafka client with the specified configuration.
// Supports SASL authentication (SCRAM-SHA-512 and PLAIN) and TLS encryption.
// The client is configured with appropriate timeouts and metadata refresh intervals.
func NewClient(brokers []string, username, password, caCertPath, saslMechanism string, insecure bool) (*Client, error) {
	var saslOption kgo.Opt
	var bc brokerConnect

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
		kgo.WithHooks(&bc),
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

// Close Closes the Kafka client connection and cleans up resources.
func (c *Client) Close() {
	c.client.Close()
}
