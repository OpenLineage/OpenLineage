// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package config

import "github.com/spf13/viper"

type Config struct {
	RetentionHours          uint   `mapstructure:"RETENTION_HOURS"`
	RetentionBytes          uint64 `mapstructure:"RETENTION_BYTES"`
	PartitionLimitHours     uint   `mapstructure:"PARTITION_LIMIT_HOURS"`
	PartitionLimitBytes     uint64 `mapstructure:"PARTITION_LIMIT_BYTES"`
	StatsDHost              string `mapstructure:"STATSD_HOST"`
	StatsDPort              uint   `mapstructure:"STATSD_PORT"`
	SqliteDatabasePath      string `mapstructure:"SQLITE_DATABASE_PATH"`
	DatabaseMigrationSource string `mapstructure:"DATABASE_MIGRATION_SOURCE"`
	FailedEventHandlerType  string `mapstructure:"FAILED_EVENT_HANDLER_TYPE"`
	TransportType           string `mapstructure:"TRANSPORT_TYPE"`
	AuthKey                 string `mapstructure:"AUTH_KEY"`
	TransportURL            string `mapstructure:"TRANSPORT_URL"`
}

func Load(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("proxy")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
