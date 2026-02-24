package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// mockDeleteGroupsResponse is a simple struct to hold the error code for testing
type mockDeleteGroupsResponse struct {
	errorCode int16
}

// mockClient implements the kafkaClient interface
type mockClient struct {
	alterConfigsResponse *kmsg.AlterConfigsResponse
	createACLsResponse   *kmsg.CreateACLsResponse
	deleteACLsResponse   *kmsg.DeleteACLsResponse
	describeACLsResponse *kmsg.DescribeACLsResponse
	deleteGroupsResponse *mockDeleteGroupsResponse
}

func (m *mockClient) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	return m.RequestWith(ctx, req)
}

func (m *mockClient) RequestWith(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	switch req.(type) {
	case *kmsg.ApiVersionsRequest:
		// Return a response advertising all ACL APIs as supported
		return &kmsg.ApiVersionsResponse{
			ApiKeys: []kmsg.ApiVersionsResponseApiKey{
				{ApiKey: 29, MinVersion: 0, MaxVersion: 3}, // DescribeACLs
				{ApiKey: 30, MinVersion: 0, MaxVersion: 3}, // CreateACLs
				{ApiKey: 31, MinVersion: 0, MaxVersion: 3}, // DeleteACLs
			},
		}, nil
	case *kmsg.AlterConfigsRequest:
		return m.alterConfigsResponse, nil
	case *kmsg.CreateACLsRequest:
		return m.createACLsResponse, nil
	case *kmsg.DeleteACLsRequest:
		return m.deleteACLsResponse, nil
	case *kmsg.DescribeACLsRequest:
		return m.describeACLsResponse, nil
	case *kmsg.DeleteGroupsRequest:
		// Create a DeleteGroupsResponse with the mock error code
		if m.deleteGroupsResponse != nil {
			resp := &kmsg.DeleteGroupsResponse{
				Groups: []kmsg.DeleteGroupsResponseGroup{
					{
						Group:     "test-group",
						ErrorCode: m.deleteGroupsResponse.errorCode,
					},
				},
			}
			return resp, nil
		}
		return nil, nil
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

// NewMockClientWithDeleteGroupsResponse creates a mock client with a DeleteGroupsResponse
func NewMockClientWithDeleteGroupsResponse(errorCode int16) kafkaClient {
	return &mockClient{
		deleteGroupsResponse: &mockDeleteGroupsResponse{
			errorCode: errorCode,
		},
	}
}
