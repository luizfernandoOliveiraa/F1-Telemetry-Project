package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type Config struct {
	UDPPort             int    `json:"udp_port"`
	KafkaBroker         string `json:"kafka_broker"`
	KafkaTopic          string `json:"kafka_topic"`
	AzureStorageAccount string `json:"azure_storage_account"`
	AzureContainer      string `json:"azure_container"`
	AzureDirectory      string `json:"azure_directory"`
	AzureAccessKey      string `json:"azure_access_key"`
}

var (
	defaultConfig = Config{
		UDPPort:     20777,
		KafkaBroker: "localhost:9092",
		KafkaTopic:  "f1-telemetry",
	}
	configLock sync.RWMutex
)

// GetDefaultConfig returns a copy of the default config
func GetDefaultConfig() Config {
	return defaultConfig
}

// LoadConfig loads the configuration from a JSON file, or creates a default one if it doesn't exist
func LoadConfig(path string) (*Config, error) {
	configLock.Lock()
	defer configLock.Unlock()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Create default config
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
		data, err := json.MarshalIndent(defaultConfig, "", "  ")
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			return nil, err
		}
		c := defaultConfig
		return &c, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	c := defaultConfig
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}

	overrideWithEnv(&c)

	return &c, nil
}

// SaveConfig saves the configuration to the specified path
func SaveConfig(path string, c *Config) error {
	configLock.Lock()
	defer configLock.Unlock()

	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func overrideWithEnv(c *Config) {
	if val := os.Getenv("UDP_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			c.UDPPort = port
		}
	}
	if val := os.Getenv("KAFKA_BROKER"); val != "" {
		c.KafkaBroker = val
	} else if val := os.Getenv("KAFKA_BROKERS"); val != "" {
		c.KafkaBroker = val
	}
	if val := os.Getenv("KAFKA_TOPIC"); val != "" {
		c.KafkaTopic = val
	}
	if val := os.Getenv("AZURE_STORAGE_ACCOUNT"); val != "" {
		c.AzureStorageAccount = val
	} else if val := os.Getenv("AZURE_STORAGE_ACCOUNT_NAME"); val != "" {
		c.AzureStorageAccount = val
	}
	if val := os.Getenv("AZURE_CONTAINER"); val != "" {
		c.AzureContainer = val
	} else if val := os.Getenv("AZURE_STORAGE_CONTAINER"); val != "" {
		c.AzureContainer = val
	}
	if val := os.Getenv("AZURE_DIRECTORY"); val != "" {
		c.AzureDirectory = val
	} else if val := os.Getenv("AZURE_STORAGE_DIRECTORY"); val != "" {
		c.AzureDirectory = val
	}
	if val := os.Getenv("AZURE_ACCESS_KEY"); val != "" {
		c.AzureAccessKey = val
	} else if val := os.Getenv("AZURE_STORAGE_ACCOUNT_KEY"); val != "" {
		c.AzureAccessKey = val
	}
}
