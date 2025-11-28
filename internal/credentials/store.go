package credentials

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zalando/go-keyring"
)

const (
	serviceName        = "kafka-admin-cli"
	activeProfileKey   = "_active_profile"
	configDirName      = ".kac"
	activeProfileFile  = "active_profile"
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

	// Track this profile
	if err := trackProfile(profileName); err != nil {
		// Non-fatal error, just log it
		fmt.Fprintf(os.Stderr, "Warning: failed to track profile: %v\n", err)
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

	// Untrack this profile
	if err := untrackProfile(profileName); err != nil {
		// Non-fatal error
		fmt.Fprintf(os.Stderr, "Warning: failed to untrack profile: %v\n", err)
	}

	// If this was the active profile, clear it
	if GetActiveProfile() == profileName {
		configDir, _ := getConfigDir()
		if configDir != "" {
			configFile := filepath.Join(configDir, activeProfileFile)
			os.Remove(configFile) // Ignore errors
		}
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

// ProfileInfo contains non-sensitive profile information
type ProfileInfo struct {
	Name          string `json:"name"`
	Brokers       string `json:"brokers"`
	Username      string `json:"username"`
	SASLMechanism string `json:"sasl_mechanism,omitempty"`
	CACertPath    string `json:"ca_cert,omitempty"`
	Insecure      bool   `json:"insecure,omitempty"`
	IsActive      bool   `json:"is_active"`
}

// List returns information about all stored profiles
// Note: go-keyring doesn't provide a native list function, so we need to track profiles separately
func List() ([]ProfileInfo, error) {
	// Try common profile names first
	commonNames := []string{"default", "dev", "staging", "prod", "production", "test", "local"}
	var profiles []ProfileInfo
	activeProfile := GetActiveProfile()

	// Check for profiles in a tracking file
	trackedProfiles, err := getTrackedProfiles()
	if err == nil && len(trackedProfiles) > 0 {
		commonNames = append(trackedProfiles, commonNames...)
	}

	seen := make(map[string]bool)
	for _, name := range commonNames {
		if seen[name] {
			continue
		}
		seen[name] = true

		profile, err := Load(name)
		if err == nil {
			profiles = append(profiles, ProfileInfo{
				Name:          name,
				Brokers:       profile.Brokers,
				Username:      profile.Username,
				SASLMechanism: profile.SASLMechanism,
				CACertPath:    profile.CACertPath,
				Insecure:      profile.Insecure,
				IsActive:      name == activeProfile,
			})
		}
	}

	return profiles, nil
}

// SetActiveProfile sets the active profile
func SetActiveProfile(profileName string) error {
	if profileName == "" {
		profileName = "default"
	}

	// Verify profile exists
	if !Exists(profileName) {
		return fmt.Errorf("profile '%s' does not exist", profileName)
	}

	// Store in config file
	configDir, err := getConfigDir()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	configFile := filepath.Join(configDir, activeProfileFile)
	if err := os.WriteFile(configFile, []byte(profileName), 0600); err != nil {
		return fmt.Errorf("failed to write active profile: %w", err)
	}

	return nil
}

// GetActiveProfile returns the name of the active profile
func GetActiveProfile() string {
	configDir, err := getConfigDir()
	if err != nil {
		return "default"
	}

	configFile := filepath.Join(configDir, activeProfileFile)
	data, err := os.ReadFile(configFile)
	if err != nil {
		return "default"
	}

	profileName := string(data)
	if profileName == "" {
		return "default"
	}

	return profileName
}

// getConfigDir returns the configuration directory path
func getConfigDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}

	return filepath.Join(homeDir, configDirName), nil
}

// trackProfile adds a profile name to the tracking file
func trackProfile(profileName string) error {
	profiles, _ := getTrackedProfiles()
	
	// Check if already tracked
	for _, p := range profiles {
		if p == profileName {
			return nil
		}
	}

	profiles = append(profiles, profileName)
	return saveTrackedProfiles(profiles)
}

// untrackProfile removes a profile name from the tracking file
func untrackProfile(profileName string) error {
	profiles, err := getTrackedProfiles()
	if err != nil {
		return nil // Nothing to untrack
	}

	var updated []string
	for _, p := range profiles {
		if p != profileName {
			updated = append(updated, p)
		}
	}

	return saveTrackedProfiles(updated)
}

// getTrackedProfiles reads the list of profile names from the tracking file
func getTrackedProfiles() ([]string, error) {
	configDir, err := getConfigDir()
	if err != nil {
		return nil, err
	}

	trackingFile := filepath.Join(configDir, "profiles.json")
	data, err := os.ReadFile(trackingFile)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var profiles []string
	if err := json.Unmarshal(data, &profiles); err != nil {
		return nil, err
	}

	return profiles, nil
}

// saveTrackedProfiles saves the list of profile names to the tracking file
func saveTrackedProfiles(profiles []string) error {
	configDir, err := getConfigDir()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(configDir, 0700); err != nil {
		return err
	}

	trackingFile := filepath.Join(configDir, "profiles.json")
	data, err := json.Marshal(profiles)
	if err != nil {
		return err
	}

	return os.WriteFile(trackingFile, data, 0600)
}
