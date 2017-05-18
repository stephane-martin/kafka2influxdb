package main

import "github.com/spf13/viper"

func set_defaults(v *viper.Viper) {
	v.SetDefault("retry_delay_ms", 30000)
	v.SetDefault("batch_size", 5000)
	v.SetDefault("batch_max_duration", 60000)
	v.SetDefault("topics", []string{"metrics_*"})
	v.SetDefault("refresh_topics", 300000)

	v.SetDefault("kafka.brokers", []string{"kafka1", "kafka2", "kafka3"})
	v.SetDefault("kafka.client_id", "kafka2influxdb")
	v.SetDefault("kafka.consumer_group", "kafka2influxdb-cg")
	v.SetDefault("kafka.version", "0.9.0.0")
	v.SetDefault("kafka.tls_enable", false)
	v.SetDefault("kafka.certificate_authority", "")
	v.SetDefault("kafka.certificate", "")
	v.SetDefault("kafka.private_key", "")
	v.SetDefault("kafka.insecure", false)
	v.SetDefault("kafka.sasl_enable", false)
	v.SetDefault("kafka.sasl_username", "")
	v.SetDefault("kafka.sasl_password", "")

	v.SetDefault("topic_conf.default.host", "http://influxdb:8086")
	v.SetDefault("topic_conf.default.auth", false)
	v.SetDefault("topic_conf.default.username", "")
	v.SetDefault("topic_conf.default.password", "")
	v.SetDefault("topic_conf.default.create_databases", false)
	v.SetDefault("topic_conf.default.admin_username", "")
	v.SetDefault("topic_conf.default.admin_password", "")
	v.SetDefault("topic_conf.default.precision", "ns")
	v.SetDefault("topic_conf.default.retention_policy", "")
	v.SetDefault("topic_conf.default.timeout", 5000)
	v.SetDefault("topic_conf.default.dbname", "default_db")
	v.SetDefault("topic_conf.default.format", "json")
	v.SetDefault("topic_conf.default.tls_enable", false)
	v.SetDefault("topic_conf.default.certificate_authority", "")
	v.SetDefault("topic_conf.default.certificate", "")
	v.SetDefault("topic_conf.default.private_key", "")
	v.SetDefault("topic_conf.default.insecure", false)

	v.SetDefault("metrics.host", "http://influxdb:8086")
	v.SetDefault("metrics.auth", false)
	v.SetDefault("metrics.username", "")
	v.SetDefault("metrics.password", "")
	v.SetDefault("metrics.create_databases", false)
	v.SetDefault("metrics.admin_username", "")
	v.SetDefault("metrics.admin_password", "")
	v.SetDefault("metrics.precision", "ns")
	v.SetDefault("metrics.retention_policy", "")
	v.SetDefault("metrics.timeout", 5000)
	v.SetDefault("metrics.dbname", "k2i_metrics")
	v.SetDefault("metrics.tls_enable", false)
	v.SetDefault("metrics.certificate_authority", "")
	v.SetDefault("metrics.certificate", "")
	v.SetDefault("metrics.private_key", "")
	v.SetDefault("metrics.insecure", false)
	v.SetDefault("metrics.enable", false)

	default_mapping := map[string]string{"*": "default"}
	v.SetDefault("mapping", []map[string]string{default_mapping})
}


