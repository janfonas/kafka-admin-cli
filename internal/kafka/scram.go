package kafka

import (
	"context"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeUserSCRAMs returns SCRAM credential metadata for the selected users.
// An empty selection is a no-op; users without credentials have an empty entry.
func (c *Client) DescribeUserSCRAMs(ctx context.Context, users []string) (map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo, error) {
	credentials := make(map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo, len(users))
	if len(users) == 0 {
		return credentials, nil
	}

	req := kmsg.NewPtrDescribeUserSCRAMCredentialsRequest()
	requested := make(map[string]struct{}, len(users))
	for _, user := range users {
		if user == "" {
			return nil, fmt.Errorf("cannot describe user SCRAM credentials: empty username")
		}
		if _, exists := requested[user]; exists {
			continue
		}
		requested[user] = struct{}{}
		entry := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
		entry.Name = user
		req.Users = append(req.Users, entry)
	}

	ctx, cancel := context.WithTimeout(ctx, ACLRequestTimeout)
	defer cancel()

	resp, err := req.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("failed to describe user SCRAM credentials (requires Kafka 2.7+ and DESCRIBE on CLUSTER, timeout=%v): %w", ACLRequestTimeout, err)
	}
	if resp.ErrorCode != 0 {
		return nil, fmt.Errorf("failed to describe user SCRAM credentials (requires Kafka 2.7+ and DESCRIBE on CLUSTER, error code %d): %w", resp.ErrorCode, kerr.ErrorForCode(resp.ErrorCode))
	}

	for _, result := range resp.Results {
		if _, exists := requested[result.User]; !exists {
			return nil, fmt.Errorf("invalid describe user SCRAM credentials response: unexpected user %q", result.User)
		}
		if _, exists := credentials[result.User]; exists {
			return nil, fmt.Errorf("invalid describe user SCRAM credentials response: duplicate user %q", result.User)
		}
		switch result.ErrorCode {
		case 0:
			credentials[result.User] = slices.Clone(result.CredentialInfos)
		case kerr.ResourceNotFound.Code:
			credentials[result.User] = nil
		default:
			return nil, fmt.Errorf("failed to describe SCRAM credentials for user %q (error code %d): %w", result.User, result.ErrorCode, kerr.ErrorForCode(result.ErrorCode))
		}
	}
	for _, user := range req.Users {
		if _, exists := credentials[user.Name]; !exists {
			return nil, fmt.Errorf("invalid describe user SCRAM credentials response: missing user %q", user.Name)
		}
	}
	return credentials, nil
}
