package kafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type scramRecordingClient struct {
	request   kmsg.Request
	ctx       context.Context
	calls     int
	response  kmsg.Response
	err       error
	requestFn func(context.Context, kmsg.Request) (kmsg.Response, error)
}

func (m *scramRecordingClient) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	m.request = req
	m.ctx = ctx
	m.calls++
	if m.requestFn != nil {
		return m.requestFn(ctx, req)
	}
	return m.response, m.err
}

func (m *scramRecordingClient) Close() {}

func TestDescribeUserSCRAMsRequestAndMetadata(t *testing.T) {
	infos := []kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo{
		{Mechanism: 1, Iterations: 4096},
		{Mechanism: 2, Iterations: 8192},
		{Mechanism: 99, Iterations: 16384},
	}
	mock := &scramRecordingClient{
		response: &kmsg.DescribeUserSCRAMCredentialsResponse{
			Results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{
				{User: "bob"},
				{User: "alice", CredentialInfos: infos},
			},
		},
	}
	users := []string{"alice", "bob", "alice", "bob"}
	got, err := NewClientWithMock(mock).DescribeUserSCRAMs(context.Background(), users)
	if err != nil {
		t.Fatal(err)
	}
	if mock.calls != 1 {
		t.Fatalf("request calls = %d, want 1", mock.calls)
	}
	req, ok := mock.request.(*kmsg.DescribeUserSCRAMCredentialsRequest)
	if !ok {
		t.Fatalf("request type = %T, want DescribeUserSCRAMCredentialsRequest", mock.request)
	}
	wantReq := kmsg.NewPtrDescribeUserSCRAMCredentialsRequest()
	for _, name := range []string{"alice", "bob"} {
		user := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
		user.Name = name
		wantReq.Users = append(wantReq.Users, user)
	}
	if !reflect.DeepEqual(req, wantReq) {
		t.Errorf("request = %#v, want %#v", req, wantReq)
	}
	if !reflect.DeepEqual(users, []string{"alice", "bob", "alice", "bob"}) {
		t.Errorf("input users changed: %v", users)
	}
	want := map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo{
		"alice": infos,
		"bob":   nil,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("credentials = %#v, want %#v", got, want)
	}
	got["alice"][0].Iterations = 1
	if infos[0].Iterations != 4096 {
		t.Error("returned credential metadata aliases the response")
	}
}

func TestDescribeUserSCRAMsEmptySelection(t *testing.T) {
	for _, tc := range []struct {
		name  string
		users []string
	}{
		{name: "nil"},
		{name: "empty", users: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &scramRecordingClient{}
			got, err := NewClientWithMock(mock).DescribeUserSCRAMs(context.Background(), tc.users)
			if err != nil || got == nil || len(got) != 0 {
				t.Fatalf("got %v, %v; want non-nil empty map and no error", got, err)
			}
			if mock.calls != 0 {
				t.Errorf("request calls = %d, want 0", mock.calls)
			}
		})
	}
}

func TestDescribeUserSCRAMsEmptyUsername(t *testing.T) {
	for _, users := range [][]string{{""}, {"alice", ""}, {"", "alice"}} {
		mock := &scramRecordingClient{}
		got, err := NewClientWithMock(mock).DescribeUserSCRAMs(context.Background(), users)
		if err == nil || !strings.Contains(err.Error(), "empty username") || got != nil {
			t.Errorf("users %q: got %v, %v; want empty username error", users, got, err)
		}
		if mock.calls != 0 {
			t.Errorf("users %q: request calls = %d, want 0", users, mock.calls)
		}
	}
}

func TestDescribeUserSCRAMsAbsence(t *testing.T) {
	mock := &scramRecordingClient{
		response: &kmsg.DescribeUserSCRAMCredentialsResponse{
			Results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{
				{User: "absent", ErrorCode: kerr.ResourceNotFound.Code},
				{User: "empty", CredentialInfos: []kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo{}},
			},
		},
	}
	got, err := NewClientWithMock(mock).DescribeUserSCRAMs(context.Background(), []string{"absent", "empty"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d users, want 2", len(got))
	}
	for _, user := range []string{"absent", "empty"} {
		if infos, ok := got[user]; !ok || len(infos) != 0 {
			t.Errorf("user %q: got %v, present %v; want empty entry", user, infos, ok)
		}
	}
}

func TestDescribeUserSCRAMsErrors(t *testing.T) {
	transportErr := errors.New("connection failed")
	for _, tc := range []struct {
		name      string
		transport error
		topCode   int16
		userCode  int16
		wantErr   error
		context   string
	}{
		{name: "transport", transport: transportErr, wantErr: transportErr, context: "Kafka 2.7+"},
		{name: "unsupported API", transport: kerr.UnsupportedVersion, wantErr: kerr.UnsupportedVersion, context: "Kafka 2.7+"},
		{name: "top authorization", topCode: kerr.ClusterAuthorizationFailed.Code, wantErr: kerr.ClusterAuthorizationFailed, context: "DESCRIBE on CLUSTER"},
		{name: "top timeout", topCode: kerr.RequestTimedOut.Code, wantErr: kerr.RequestTimedOut},
		{name: "top not found is not absence", topCode: kerr.ResourceNotFound.Code, wantErr: kerr.ResourceNotFound},
		{name: "top unknown code", topCode: 32767, wantErr: kerr.UnknownServerError, context: "32767"},
		{name: "user timeout", userCode: kerr.RequestTimedOut.Code, wantErr: kerr.RequestTimedOut, context: `"alice"`},
		{name: "user authorization", userCode: kerr.ClusterAuthorizationFailed.Code, wantErr: kerr.ClusterAuthorizationFailed, context: `"alice"`},
		{name: "user duplicate resource", userCode: kerr.DuplicateResource.Code, wantErr: kerr.DuplicateResource, context: `"alice"`},
		{name: "user unknown code", userCode: 32767, wantErr: kerr.UnknownServerError, context: "32767"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &scramRecordingClient{
				err: tc.transport,
				response: &kmsg.DescribeUserSCRAMCredentialsResponse{
					ErrorCode: tc.topCode,
					Results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{
						{User: "alice", ErrorCode: tc.userCode},
					},
				},
			}
			got, err := NewClientWithMock(mock).DescribeUserSCRAMs(context.Background(), []string{"alice"})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want wrapped %v", err, tc.wantErr)
			}
			if got != nil {
				t.Errorf("got partial credentials on failure: %v", got)
			}
			if !strings.Contains(err.Error(), tc.context) {
				t.Errorf("error %q does not contain %q", err, tc.context)
			}
			if mock.calls != 1 {
				t.Errorf("request calls = %d, want 1", mock.calls)
			}
		})
	}
}

func TestDescribeUserSCRAMsInvalidResponse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		results []kmsg.DescribeUserSCRAMCredentialsResponseResult
		wantErr string
	}{
		{name: "missing all", wantErr: `missing user "alice"`},
		{name: "missing one", results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{{User: "alice"}}, wantErr: `missing user "bob"`},
		{name: "duplicate", results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{{User: "alice"}, {User: "alice"}}, wantErr: `duplicate user "alice"`},
		{name: "duplicate absent", results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{{User: "alice", ErrorCode: kerr.ResourceNotFound.Code}, {User: "alice"}}, wantErr: `duplicate user "alice"`},
		{name: "unexpected", results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{{User: "mallory"}}, wantErr: `unexpected user "mallory"`},
		{name: "unexpected absent", results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{{User: "mallory", ErrorCode: kerr.ResourceNotFound.Code}}, wantErr: `unexpected user "mallory"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &scramRecordingClient{
				response: &kmsg.DescribeUserSCRAMCredentialsResponse{Results: tc.results},
			}
			got, err := NewClientWithMock(mock).DescribeUserSCRAMs(context.Background(), []string{"alice", "bob"})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
			if got != nil {
				t.Errorf("got partial credentials on failure: %v", got)
			}
		})
	}
}

func TestDescribeUserSCRAMsContext(t *testing.T) {
	t.Run("bounded deadline", func(t *testing.T) {
		mock := &scramRecordingClient{
			response: &kmsg.DescribeUserSCRAMCredentialsResponse{
				Results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{{User: "alice"}},
			},
		}
		before := time.Now()
		_, err := NewClientWithMock(mock).DescribeUserSCRAMs(context.Background(), []string{"alice"})
		after := time.Now()
		if err != nil {
			t.Fatal(err)
		}
		deadline, ok := mock.ctx.Deadline()
		if !ok || deadline.Before(before.Add(ACLRequestTimeout)) || deadline.After(after.Add(ACLRequestTimeout)) {
			t.Errorf("request deadline = %v, present %v; want bounded by %v", deadline, ok, ACLRequestTimeout)
		}
		if !errors.Is(mock.ctx.Err(), context.Canceled) {
			t.Errorf("request context not canceled after return: %v", mock.ctx.Err())
		}
	})

	t.Run("earlier parent deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		mock := &scramRecordingClient{
			response: &kmsg.DescribeUserSCRAMCredentialsResponse{
				Results: []kmsg.DescribeUserSCRAMCredentialsResponseResult{{User: "alice"}},
			},
		}
		if _, err := NewClientWithMock(mock).DescribeUserSCRAMs(ctx, []string{"alice"}); err != nil {
			t.Fatal(err)
		}
		want, _ := ctx.Deadline()
		got, ok := mock.ctx.Deadline()
		if !ok || !got.Equal(want) {
			t.Errorf("request deadline = %v, present %v; want %v", got, ok, want)
		}
		if ctx.Err() != nil {
			t.Errorf("parent context canceled by request: %v", ctx.Err())
		}
	})

	t.Run("parent cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mock := &scramRecordingClient{
			requestFn: func(requestCtx context.Context, _ kmsg.Request) (kmsg.Response, error) {
				cancel()
				return nil, requestCtx.Err()
			},
		}
		_, err := NewClientWithMock(mock).DescribeUserSCRAMs(ctx, []string{"alice"})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want wrapped context.Canceled", err)
		}
	})

	t.Run("expired parent deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		mock := &scramRecordingClient{
			requestFn: func(requestCtx context.Context, _ kmsg.Request) (kmsg.Response, error) {
				return nil, requestCtx.Err()
			},
		}
		_, err := NewClientWithMock(mock).DescribeUserSCRAMs(ctx, []string{"alice"})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want wrapped context.DeadlineExceeded", err)
		}
	})
}
