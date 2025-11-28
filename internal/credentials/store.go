package credentials

import (
	"encoding/json"
	"fmt"

	"github.com/zalando/go-keyring"
)

const (
	serviceName = "kafka-admin-cli"
)

type Profile struct {
	Brokers       string `json:"brokers"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	SASLMechanism string `json:"sasl_mechanism,omitempty"`
	CACertPath    string `json:"ca_cert,omitempty"`
	Insecure      bool   `json:"insecure,omitempty"`
}

// Store saves a profile to the OS keyring
func Store(profileName string, profile *Profile) error {
	if profileName == "" {
		profileName = "default"
	}

	data, err := json.Marshal(profile)
	if err != nil {
		return fmt.Errorf("failed to marshal profile: %w", err)
	}

	err = keyring.Set(serviceName, profileName, string(data))
	if err != nil {
		return fmt.Errorf("failed to store credentials in keyring: %w", err)
	}

	return nil
}

// Load retrieves a profile from the OS keyring
func Load(profileName string) (*Profile, error) {
	if profileName == "" {
		profileName = "default"
	}

	data, err := keyring.Get(serviceName, profileName)
	if err != nil {
		if err == keyring.ErrNotFound {
			return nil, fmt.Errorf("profile '%s' not found. Use 'kac login' to save credentials", profileName)
		}
		return nil, fmt.Errorf("failed to load credentials from keyring: %w", err)
	}

	var profile Profile
	err = json.Unmarshal([]byte(data), &profile)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal profile: %w", err)
	}

	return &profile, nil
}

// Delete removes a profile from the OS keyring
func Delete(profileName string) error {
	if profileName == "" {
		profileName = "default"
	}

	err := keyring.Delete(serviceName, profileName)
	if err != nil {
		if err == keyring.ErrNotFound {
			return fmt.Errorf("profile '%s' not found", profileName)
		}
		return fmt.Errorf("failed to delete credentials from keyring: %w", err)
	}

	return nil
}

// Exists checks if a profile exists in the keyring
func Exists(profileName string) bool {
	if profileName == "" {
		profileName = "default"
	}

	_, err := keyring.Get(serviceName, profileName)
	return err == nil
}
