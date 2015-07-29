package collectd

import (
	"time"

	"github.com/influxdb/influxdb/toml"
)

const (
	DefaultAMQPAddress = "localhost"

	DefaultAMQPPort = 5672

	DefaultUser = "guest"

	DefaultPass = "guest"

	DefaultExchange = "metrics"

	DefaultVHost = "metrics/"

	DefaultKey = "collectd"

	DefaultQueue = "collectd-queue"

	DefaultDatabase = "collectd"

	DefaultRetentionPolicy = ""

	DefaultBatchSize = 5000

	DefaultBatchDuration = toml.Duration(10 * time.Second)
)

// Config represents a configuration for the collectd service.
type Config struct {
	Enabled         bool          `toml:"enabled"`
	AMQPAddress     string        `toml:"amqp-address"`
	AMQPPort        int           `toml:"amqp-port"`
	User            string        `toml:"user"`
	Pass            string        `toml:"pass"`
	Exchange        string        `toml:"exchange"`
	VHost           string        `toml:"vhost"`
	Key             string        `toml:"key"`
	Queue           string        `toml:"queue"`
	Database        string        `toml:"database"`
	RetentionPolicy string        `toml:"retention-policy"`
	BatchSize       int           `toml:"batch-size"`
	BatchDuration   toml.Duration `toml:"batch-timeout"`
}

// NewConfig returns a new instance of Config with defaults.
func NewConfig() Config {
	return Config{
		AMQPAddress:     DefaultAMQPAddress,
		AMQPPort:        DefaultAMQPPort,
		User:            DefaultUser,
		Pass:            DefaultPass,
		Exchange:        DefaultExchange,
		VHost:           DefaultVHost,
		Key:             DefaultKey,
		Queue:           DefaultQueue,
		Database:        DefaultDatabase,
		RetentionPolicy: DefaultRetentionPolicy,
		BatchSize:       DefaultBatchSize,
		BatchDuration:   DefaultBatchDuration,
	}
}
