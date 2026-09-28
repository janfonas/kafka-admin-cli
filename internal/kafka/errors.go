package kafka

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
)

// requestError wraps a non-zero Kafka error code so callers can match it with errors.Is.
// Every non-zero code is a failure, including retriable ones such as REQUEST_TIMED_OUT.
func requestError(operation string, code int16, brokerMessage *string) error {
	return kafkaError(operation, code, brokerMessage, "")
}

// aclError is requestError with remediation hints for ACL-specific failures.
func aclError(operation string, code int16, brokerMessage *string) error {
	hint := ""
	switch err := kerr.ErrorForCode(code); {
	case errors.Is(err, kerr.SecurityDisabled):
		hint = "no ACL authorizer is configured. For Strimzi, set 'authorization' in the Kafka custom resource (e.g., type: simple)"
	case errors.Is(err, kerr.ClusterAuthorizationFailed):
		hint = "the authenticated user lacks permission on the 'Cluster' resource (Describe to read ACLs, Alter to change them)"
	}
	return kafkaError(operation, code, brokerMessage, hint)
}

func kafkaError(operation string, code int16, brokerMessage *string, hint string) error {
	detail := fmt.Sprintf("error code %d", code)
	if brokerMessage != nil && *brokerMessage != "" {
		detail += fmt.Sprintf(", broker message %q", *brokerMessage)
	}
	if hint != "" {
		detail += "; " + hint
	}
	return fmt.Errorf("failed to %s (%s): %w", operation, detail, kerr.ErrorForCode(code))
}
