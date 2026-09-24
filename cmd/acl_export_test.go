package cmd

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.yaml.in/yaml/v3"
)

type fakeSCRAMDescriber struct {
	calls       int
	users       []string
	ctx         context.Context
	credentials map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo
	err         error
}

func (f *fakeSCRAMDescriber) DescribeUserSCRAMs(ctx context.Context, users []string) (map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo, error) {
	f.calls++
	f.users = append([]string(nil), users...)
	f.ctx = ctx
	return f.credentials, f.err
}

func TestExportACLStrimziDiscoverSCRAM(t *testing.T) {
	client := &fakeSCRAMDescriber{
		credentials: map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo{
			"superuser": {{Mechanism: 1}, {Mechanism: 2, Iterations: 4096}},
			"external":  nil,
		},
	}
	resources := aclResources("User:CN=managed-user", "User:superuser", "User:external", "User:superuser")
	resources[0].ACLs[3].Operation = kmsg.ACLOperationWrite
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out bytes.Buffer
	err := exportACLStrimzi(ctx, &out, client, resources, aclExportOptions{
		discoverSCRAM: true, tlsAuthentication: tlsAuthenticationManaged,
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.calls != 1 || !reflect.DeepEqual(client.users, []string{"superuser", "external"}) || client.ctx != ctx {
		t.Fatalf("discovery must query only unique non-TLS usernames with the command context: %+v", client)
	}
	documents := decodeStrimziDocuments[strimziUserSpec](t, out.Bytes())
	if len(documents) != 3 {
		t.Fatalf("expected three user documents, got %d", len(documents))
	}
	for i, want := range []string{"tls", "scram-sha-512", ""} {
		got := ""
		if documents[i].Spec.Authentication != nil {
			got = documents[i].Spec.Authentication.Type
		}
		if got != want {
			t.Errorf("document %d authentication: got %q, want %q", i, got, want)
		}
	}
	if documents[0].Metadata.Name != "managed-user" || documents[1].Metadata.Name != "superuser" {
		t.Error("authentication options changed principal names")
	}
	if got := documents[1].Spec.Authorization.ACLs[0].Operations; !reflect.DeepEqual(got, []string{"Read", "Write"}) {
		t.Errorf("SCRAM discovery changed ACL grouping: %v", got)
	}
	if strings.Contains(out.String(), "password:") {
		t.Fatal("discovery must not invent password configuration")
	}
	if strings.Count(out.String(), "\n---\n") != 2 {
		t.Fatal("mixed authentication exports must remain separate YAML documents")
	}
}

func TestExportACLStrimziSkipsUnrequestedDiscovery(t *testing.T) {
	tests := []struct {
		name       string
		principals []string
		options    aclExportOptions
	}{
		{"default", []string{"User:superuser", "User:CN=tls-user"}, aclExportOptions{}},
		{"TLS override only", []string{"User:superuser", "User:CN=tls-user"}, aclExportOptions{tlsAuthentication: tlsAuthenticationManaged}},
		{"TLS only", []string{"User:CN=tls-user"}, aclExportOptions{discoverSCRAM: true}},
		{"empty export", nil, aclExportOptions{discoverSCRAM: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeSCRAMDescriber{err: errors.New("unexpected discovery")}
			var out bytes.Buffer
			if err := exportACLStrimzi(context.Background(), &out, client, aclResources(tt.principals...), tt.options); err != nil {
				t.Fatal(err)
			}
			if client.calls != 0 {
				t.Fatal("unrequested discovery must not query the broker")
			}
			if strings.Contains(out.String(), "scram-sha-512") {
				t.Fatal("non-discovery export must not assume SCRAM authentication")
			}
		})
	}
}

func TestExportACLStrimziDiscoveryErrors(t *testing.T) {
	discoveryError := errors.New("not authorized to describe credentials")
	tests := []struct {
		name        string
		client      fakeSCRAMDescriber
		errContains string
	}{
		{"broker failure", fakeSCRAMDescriber{err: discoveryError}, "cannot discover SCRAM"},
		{"missing user", fakeSCRAMDescriber{}, "no result for user"},
		{"unsupported SHA-256", fakeSCRAMDescriber{credentials: map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo{
			"superuser": {{Mechanism: 1}},
		}}, "no SCRAM-SHA-512 credential"},
		{"unknown mechanism", fakeSCRAMDescriber{credentials: map[string][]kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo{
			"superuser": {{Mechanism: 99}},
		}}, "no SCRAM-SHA-512 credential"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := exportACLStrimzi(context.Background(), &out, &tt.client, aclResources("User:CN=valid-tls", "User:superuser"), aclExportOptions{discoverSCRAM: true})
			if err == nil || !strings.Contains(err.Error(), tt.errContains) {
				t.Fatalf("expected %q error, got %v", tt.errContains, err)
			}
			if tt.client.err != nil && !errors.Is(err, discoveryError) {
				t.Errorf("discovery error must be preserved: %v", err)
			}
			if out.Len() != 0 {
				t.Fatalf("discovery failure wrote partial manifests: %s", &out)
			}
		})
	}
}

func TestExportACLStrimziRejectsInvalidInputBeforeDiscovery(t *testing.T) {
	tests := []struct {
		name      string
		principal string
		options   aclExportOptions
	}{
		{"invalid name", "User:Bad_User", aclExportOptions{discoverSCRAM: true}},
		{"invalid TLS mode", "User:alice", aclExportOptions{discoverSCRAM: true, tlsAuthentication: "scram-sha-512"}},
		{"managed CN too long", "User:CN=" + strings.Repeat("a", 65), aclExportOptions{discoverSCRAM: true, tlsAuthentication: tlsAuthenticationManaged}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeSCRAMDescriber{}
			var out bytes.Buffer
			if err := exportACLStrimzi(context.Background(), &out, client, aclResources(tt.principal), tt.options); err == nil {
				t.Fatal("expected invalid export input to fail")
			}
			if client.calls != 0 || out.Len() != 0 {
				t.Fatal("invalid input must not query the broker or emit manifests")
			}
		})
	}
}

func TestFormatACLStrimziTLSModes(t *testing.T) {
	tests := []struct {
		mode string
		size int
		want string
		fail bool
	}{
		{"", 65, "tls-external", false},
		{tlsAuthenticationExternal, 65, "tls-external", false},
		{tlsAuthenticationManaged, 64, "tls", false},
		{tlsAuthenticationManaged, 65, "", true},
		{"invalid", 5, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.mode+"/"+strings.Repeat("a", tt.size), func(t *testing.T) {
			var out bytes.Buffer
			err := formatACLStrimzi(&out, aclResources("User:CN="+strings.Repeat("a", tt.size)), aclExportOptions{tlsAuthentication: tt.mode})
			if tt.fail {
				if err == nil || out.Len() != 0 {
					t.Fatalf("invalid TLS configuration must fail without output: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			documents := decodeStrimziDocuments[strimziUserSpec](t, out.Bytes())
			if len(documents) != 1 || documents[0].Spec.Authentication == nil || documents[0].Spec.Authentication.Type != tt.want {
				t.Fatalf("unexpected authentication: %+v", documents)
			}
		})
	}
}

func TestFormatACLStrimziClusterResourceSchema(t *testing.T) {
	resources := aclResources("User:superuser", "User:superuser")
	resources[0].ResourceType = kmsg.ACLResourceTypeCluster
	resources[0].ResourceName = "kafka-cluster"
	resources[0].ACLs[0].Operation = kmsg.ACLOperationDescribe
	resources[0].ACLs[1].Operation = kmsg.ACLOperationAlter
	var out bytes.Buffer
	if err := formatACLStrimzi(&out, resources, aclExportOptions{}); err != nil {
		t.Fatal(err)
	}
	var document struct {
		Spec struct {
			Authorization struct {
				ACLs []struct {
					Resource   map[string]any `yaml:"resource"`
					Operations []string       `yaml:"operations"`
				} `yaml:"acls"`
			} `yaml:"authorization"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(out.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	acls := document.Spec.Authorization.ACLs
	if len(acls) != 1 {
		t.Fatalf("expected one merged cluster ACL, got %d", len(acls))
	}
	if !reflect.DeepEqual(acls[0].Resource, map[string]any{"type": "cluster"}) {
		t.Errorf("cluster resources must contain only type: %+v", acls[0].Resource)
	}
	if !reflect.DeepEqual(acls[0].Operations, []string{"Describe", "Alter"}) {
		t.Errorf("unexpected cluster operations: %v", acls[0].Operations)
	}
}

func TestACLExportFlags(t *testing.T) {
	for _, newCommand := range []func() *cobra.Command{newGetACLCmd, newGetACLsCmd} {
		t.Run(newCommand().Name(), func(t *testing.T) {
			cmd := newCommand()
			options, err := readACLExportOptions(cmd, outputTable)
			if err != nil || options.discoverSCRAM || options.tlsAuthentication != tlsAuthenticationExternal {
				t.Fatalf("unexpected defaults: %+v, %v", options, err)
			}
			if err := cmd.ParseFlags([]string{"-o", "strimzi", "--discover-scram", "--tls-authentication", "tls"}); err != nil {
				t.Fatal(err)
			}
			options, err = readACLExportOptions(cmd, outputStrimzi)
			if err != nil || !options.discoverSCRAM || options.tlsAuthentication != tlsAuthenticationManaged {
				t.Fatalf("unexpected parsed options: %+v, %v", options, err)
			}
			completion, found := cmd.GetFlagCompletionFunc("tls-authentication")
			if !found {
				t.Fatal("TLS authentication flag lacks shell completion")
			}
			values, directive := completion(cmd, nil, "")
			if !reflect.DeepEqual(values, []string{"tls-external", "tls"}) || directive != cobra.ShellCompDirectiveNoFileComp {
				t.Errorf("unexpected completion: %v, %v", values, directive)
			}
		})
	}
}

func TestACLExportFlagsRejectInvalidCommands(t *testing.T) {
	for _, command := range []string{"acl", "acls"} {
		for _, flags := range [][]string{
			{"--discover-scram"},
			{"--tls-authentication", "tls"},
			{"-o", "strimzi", "--tls-authentication", "invalid"},
			{"-o", "strimzi", "--tls-authentication", ""},
		} {
			t.Run(command+"/"+strings.Join(flags, " "), func(t *testing.T) {
				output, err := executeCommand(newGetCmd(), append([]string{command}, flags...)...)
				if err == nil || (!strings.Contains(err.Error(), "require --output strimzi") && !strings.Contains(err.Error(), "invalid --tls-authentication")) {
					t.Fatalf("expected flag validation error before connecting, got %v", err)
				}
				if strings.Contains(output, "apiVersion:") {
					t.Fatal("invalid flags emitted a manifest")
				}
			})
		}
	}
}
