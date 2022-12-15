package config

import "github.com/spf13/viper"

type Config struct {
	RetentionHours         uint   `mapstructure:"RETENTION_HOURS"`
	RetentionBytes         uint64 `mapstructure:"RETENTION_BYTES"`
	StatsDHost             string `mapstructure:"STATSD_HOST"`
	StatsDPort             uint   `mapstructure:"STATSD_PORT"`
	SqliteDatabasePath     string `mapstructure:"SQLITE_DATABASE_PATH"`
	FailedEventHandlerType string `mapstructure:"FAILED_EVENT_HANDLER_TYPE"`
	TransportType          string `mapstructure:"TRANSPORT_TYPE"`
	AuthKey                string `mapstructure:"AUTH_KEY"`
	TransportURL           string `mapstructure:"TRANSPORT_URL"`
}

func Load(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
