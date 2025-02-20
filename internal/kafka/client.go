package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
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
