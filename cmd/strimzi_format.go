package cmd

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"slices"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"
)

const maxKubernetesNameLength = 253

// v1beta2 stays the default for existing users; Strimzi releases that serve only v1 need the flag.
const (
	strimziAPIVersionV1Beta2 = "kafka.strimzi.io/v1beta2"
	strimziAPIVersionV1      = "kafka.strimzi.io/v1"
)

var strimziAPIVersions = []string{strimziAPIVersionV1Beta2, strimziAPIVersionV1}

func addStrimziAPIVersionFlag(cmd *cobra.Command) {
	cmd.Flags().String("strimzi-api-version", strimziAPIVersionV1Beta2, "Strimzi API version for --output strimzi (kafka.strimzi.io/v1beta2, kafka.strimzi.io/v1)")
	_ = cmd.RegisterFlagCompletionFunc("strimzi-api-version", cobra.FixedCompletions(strimziAPIVersions, cobra.ShellCompDirectiveNoFileComp))
}

func readStrimziAPIVersion(cmd *cobra.Command, outputFormat string) (string, error) {
	apiVersion, err := cmd.Flags().GetString("strimzi-api-version")
	if err != nil {
		return "", err
	}
	if !slices.Contains(strimziAPIVersions, apiVersion) {
		return "", fmt.Errorf("invalid --strimzi-api-version %q: use %s or %s", apiVersion, strimziAPIVersionV1Beta2, strimziAPIVersionV1)
	}
	if outputFormat != outputStrimzi && cmd.Flags().Changed("strimzi-api-version") {
		return "", fmt.Errorf("--strimzi-api-version requires --output strimzi")
	}
	return apiVersion, nil
}

func strimziAPIVersionOrDefault(apiVersion string) string {
	if apiVersion == "" {
		return strimziAPIVersionV1Beta2
	}
	return apiVersion
}

var kubernetesNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

func isValidKubernetesName(name string) bool {
	return len(name) <= maxKubernetesNameLength && kubernetesNamePattern.MatchString(name)
}

type strimziMetadata struct {
	Name string `yaml:"name"`
}

type strimziManifest[T any] struct {
	APIVersion string          `yaml:"apiVersion"`
	Kind       string          `yaml:"kind"`
	Metadata   strimziMetadata `yaml:"metadata"`
	Spec       T               `yaml:"spec"`
}

func writeStrimziManifests[T any](w io.Writer, manifests []strimziManifest[T]) error {
	if len(manifests) == 0 {
		return nil
	}
	// Finish serialization before writing anything that could be piped to kubectl.
	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	for _, manifest := range manifests {
		if err := encoder.Encode(manifest); err != nil {
			return fmt.Errorf("encode Strimzi manifest: %w", err)
		}
	}
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("finish Strimzi manifests: %w", err)
	}
	if _, err := buf.WriteTo(w); err != nil {
		return fmt.Errorf("write Strimzi manifests: %w", err)
	}
	return nil
}
