package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// mockClient implements the kafkaClient interface
type mockClient struct {
	alterConfigsResponse *kmsg.AlterConfigsResponse
	createACLsResponse   *kmsg.CreateACLsResponse
	deleteACLsResponse   *kmsg.DeleteACLsResponse
	describeACLsResponse *kmsg.DescribeACLsResponse
}

func (m *mockClient) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	return m.RequestWith(ctx, req)
}

func (m *mockClient) RequestWith(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	switch req.(type) {
	case *kmsg.AlterConfigsRequest:
		return m.alterConfigsResponse, nil
	case *kmsg.CreateACLsRequest:
		return m.createACLsResponse, nil
	case *kmsg.DeleteACLsRequest:
		return m.deleteACLsResponse, nil
	case *kmsg.DescribeACLsRequest:
		return m.describeACLsResponse, nil
	default:
		return nil, nil
	}
}

func (m *mockClient) Close() {}

// newMockClient creates a new mock client with the given responses
func newMockClient(responses ...kmsg.Response) kafkaClient {
	mock := &mockClient{}
	for _, resp := range responses {
		switch r := resp.(type) {
		case *kmsg.AlterConfigsResponse:
			mock.alterConfigsResponse = r
		case *kmsg.CreateACLsResponse:
			mock.createACLsResponse = r
		case *kmsg.DeleteACLsResponse:
			mock.deleteACLsResponse = r
		case *kmsg.DescribeACLsResponse:
			mock.describeACLsResponse = r
		}
	}
	return mock
}
