package cmd

import (
	"bytes"
	"fmt"
	"io"
	"regexp"

	"go.yaml.in/yaml/v3"
)

const maxKubernetesNameLength = 253

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
