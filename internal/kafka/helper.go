package kafka

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// parseURL Parses a broker address string into a valid Kafka broker URL with port.
// If no port is specified, defaults to 9092. Handles IPv6 addresses by wrapping them in brackets.
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

// validateSASLMechanism Validates if the provided SASL mechanism is supported.
// Currently supports SCRAM-SHA-512 and PLAIN authentication mechanisms.
func validateSASLMechanism(mechanism string) error {
	switch mechanism {
	case "SCRAM-SHA-512", "PLAIN":
		return nil
	default:
		return fmt.Errorf("unsupported SASL mechanism: %s", mechanism)
	}
}

// configureSASL Creates a SASL authentication mechanism based on the provided credentials.
// Returns either a SCRAM-SHA-512 or PLAIN authenticator depending on the mechanism parameter.
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
