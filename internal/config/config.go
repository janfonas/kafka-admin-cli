package config

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers []string
	SASL    *kgo.SASL
}

func NewConfig(brokers []string, sasl *kgo.SASL) *Config {
	return &Config{
		Brokers: brokers,
		SASL:    sasl,
	}
}
