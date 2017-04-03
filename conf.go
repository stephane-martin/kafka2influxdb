package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	//"io/ioutil"
	"math"
	"strconv"
	"strings"
	"time"
	//"unicode/utf8"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gobwas/glob"
	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/go-version"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/spf13/viper"
)

func NewConfig() *GConfig {
	kafka_tls_conf := KafkaTLSConf{}
	kafka_sasl_conf := KafkaSASLConf{}
	kafka_conf := KafkaConf{Brokers: []string{}, TLS: kafka_tls_conf, SASL: kafka_sasl_conf}
	return &GConfig{
		Topics:     []string{},
		Mapping:    []map[string]string{},
		Kafka:      kafka_conf,
		TopicConfs: map[string]TopicConf{},
	}
}

type GConfig struct {
	RetryDelay       uint32               `mapstructure:"retry_delay_ms" toml:"retry_delay_ms"`
	Logformat        string               `mapstructure:"logformat" toml:"logformat"`
	BatchSize        uint32               `mapstructure:"batch_size" toml:"batch_size"`
	BatchMaxDuration uint32               `mapstructure:"batch_max_duration" toml:"batch_max_duration"`
	Topics           []string             `mapstructure:"topics" toml:"topics"`
	RefreshTopics    uint32               `mapstructure:"refresh_topics" toml:"refresh_topics"`
	Mapping          []map[string]string  `mapstructure:"mapping" toml:"mapping"`
	Kafka            KafkaConf            `mapstructure:"kafka" toml:"kafka"`
	TopicConfs       map[string]TopicConf `mapstructure:"topic_conf" toml:"topic_conf"`
}

type TopicConf struct {
	Host            string        `mapstructure:"host" toml:"host"`
	Auth            bool          `mapstructure:"auth" toml:"auth"`
	Username        string        `mapstructure:"username" toml:"username"`
	Password        string        `mapstructure:"password" toml:"password"`
	CreateDatabases bool          `mapstructure:"create_databases" toml:"create_databases"`
	AdminUsername   string        `mapstructure:"admin_username" toml:"admin_username"`
	AdminPassword   string        `mapstructure:"admin_password" toml:"admin_password"`
	Precision       string        `mapstructure:"precision" toml:"precision"`
	RetentionPolicy string        `mapstructure:"retention_policy" toml:"retention_policy"`
	Timeout         uint32        `mapstructure:"timeout" toml:"timeout"`
	TLS             InfluxTLSConf `mapstructure:"tls" toml:"tls"`
	DatabaseName    string        `mapstructure:"dbname" toml:"dbname"`
	Format          string        `mapstructure:"format" toml:"format"`
}

type InfluxTLSConf struct {
	Enable               bool   `mapstructure:"enable" toml:"enable"`
	CertificateAuthority string `mapstructure:"certificate_authority" toml:"certificate_authority"`
	Certificate          string `mapstructure:"certificate" toml:"certificate"`
	PrivateKey           string `mapstructure:"private_key" toml:"private_key"`
	InsecureSkipVerify   bool   `mapstructure:"insecure" toml:"insecure"`
}

type KafkaConf struct {
	Brokers       []string `mapstructure:"brokers" toml:"brokers"`
	ClientID      string   `mapstructure:"client_id" toml:"client_id"`
	ConsumerGroup string   `mapstructure:"consumer_group" toml:"consumer_group"`
	Version       string   `mapstructure:"version" toml:"version"`
	cVersion      sarama.KafkaVersion
	TLS           KafkaTLSConf  `mapstructure:"tls" toml:"tls"`
	SASL          KafkaSASLConf `mapstructure:"sasl" toml:"sasl"`
}

type KafkaTLSConf struct {
	Enable               bool   `mapstructure:"enable" toml:"enable"`
	CertificateAuthority string `mapstructure:"certificate_authority" toml:"certificate_authority"`
	Certificate          string `mapstructure:"certificate" toml:"certificate"`
	PrivateKey           string `mapstructure:"private_key" toml:"private_key"`
	InsecureSkipVerify   bool   `mapstructure:"insecure" toml:"insecure"`
}

type KafkaSASLConf struct {
	Enable   bool   `mapstructure:"enable" toml:"enable"`
	Username string `mapstructure:"username" toml:"username"`
	Password string `mapstructure:"password" toml:"password"`
}

func normalize(s string) string {
	return strings.Trim(strings.ToLower(s), " ")
}

func (conf *GConfig) sanitize() {
	for mapping_name, topic_conf := range conf.TopicConfs {
		if topic_conf.Auth {
			if len(topic_conf.Username) == 0 {
				topic_conf.Username = topic_conf.AdminUsername
				topic_conf.Password = topic_conf.AdminPassword
			}
			if len(topic_conf.AdminUsername) == 0 {
				topic_conf.AdminUsername = topic_conf.Username
				topic_conf.AdminPassword = topic_conf.Password
			}
		}
		topic_conf.Host = strings.Trim(topic_conf.Host, " ")
		topic_conf.Precision = normalize(topic_conf.Precision)
		topic_conf.Username = strings.Trim(topic_conf.Username, " ")
		topic_conf.AdminUsername = strings.Trim(topic_conf.AdminUsername, " ")
		topic_conf.RetentionPolicy = strings.Trim(topic_conf.RetentionPolicy, " ")
		topic_conf.DatabaseName = strings.Trim(topic_conf.DatabaseName, " ")
		topic_conf.Format = strings.Trim(topic_conf.Format, " ")

		conf.TopicConfs[mapping_name] = topic_conf
	}

	conf.Logformat = normalize(conf.Logformat)
	// todo: sanitize topics

	// todo: sanitize brokers
	conf.Kafka.ClientID = strings.Trim(conf.Kafka.ClientID, " ")
	conf.Kafka.ConsumerGroup = strings.Trim(conf.Kafka.ConsumerGroup, " ")
	conf.Kafka.Version = strings.Trim(conf.Kafka.Version, " ")
	conf.Kafka.SASL.Username = strings.Trim(conf.Kafka.SASL.Username, " ")

}

func (conf *GConfig) check() error {
	conf.sanitize()

	numbers_s := strings.Split(conf.Kafka.Version, ".")
	for _, number_s := range numbers_s {
		_, err := strconv.ParseUint(number_s, 10, 8)
		if err != nil {
			return fmt.Errorf("Kafka version has improper format: '%s'", conf.Kafka.Version)
		}
	}

	v, err := version.NewVersion(conf.Kafka.Version)
	if err != nil {
		return errwrap.Wrapf("Failed to parse Kafka version", err)
	}
	v_0_10_1_0, _ := version.NewVersion("0.10.1.0")
	v_0_10_0_1, _ := version.NewVersion("0.10.0.1")
	v_0_10_0_0, _ := version.NewVersion("0.10.0.0")
	v_0_9_0_1, _ := version.NewVersion("0.9.0.1")
	v_0_9_0_0, _ := version.NewVersion("0.9.0.0")

	if v.Compare(v_0_10_1_0) >= 0 {
		conf.Kafka.cVersion = sarama.V0_10_1_0
	} else if v.Compare(v_0_10_0_1) >= 0 {
		conf.Kafka.cVersion = sarama.V0_10_0_1
	} else if v.Compare(v_0_10_0_0) >= 0 {
		conf.Kafka.cVersion = sarama.V0_10_0_0
	} else if v.Compare(v_0_9_0_1) >= 0 {
		conf.Kafka.cVersion = sarama.V0_9_0_1
	} else if v.Compare(v_0_9_0_0) >= 0 {
		conf.Kafka.cVersion = sarama.V0_9_0_0
	} else {
		return fmt.Errorf("Kafka is not recent enough. Needs at least 0.9")
	}

	if !(conf.Logformat == "json" || conf.Logformat == "text") {
		return fmt.Errorf("Logformat must be 'json' or 'text'")
	}

	if len(conf.Topics) == 0 {
		return fmt.Errorf("Provide a list of kafka topics to consume from")
	}

	if len(conf.Kafka.Brokers) == 0 {
		return fmt.Errorf("Provide a list of Kafka brokers to connect to")
	}

	if conf.BatchSize > math.MaxInt32 {
		return fmt.Errorf("BatchSize %d is too big. Max = %d", conf.BatchSize, math.MaxInt32)
	}

	// https://docs.influxdata.com/influxdb/v1.2/tools/api/#write: precision=[ns,u,ms,s,m,h]
	valid_precisions := map[string]bool{
		"s":  true,
		"ms": true,
		"u":  true,
		"ns": true,
		"m":  true,
		"h":  true,
	}

	for _, topic_conf := range conf.TopicConfs {
		if !(topic_conf.Format == "json" || topic_conf.Format == "influx") {
			return fmt.Errorf("Kafka format must be 'influx' or 'json'")
		}
		if topic_conf.Auth {
			if len(topic_conf.Username) == 0 || len(topic_conf.Password) == 0 {
				return fmt.Errorf("InfluxDB authentication is requested but username or password is empty")
			}
			if topic_conf.CreateDatabases && (len(topic_conf.AdminUsername) == 0 || len(topic_conf.AdminPassword) == 0) {
				return fmt.Errorf("InfluxDB authentication is requested, should create InfluxDB databases, but admin username or password is empty")
			}
		}
		if !strings.HasPrefix(topic_conf.Host, "http://") {
			return fmt.Errorf("Incorrect format for InfluxDB host")
		}

		if !valid_precisions[topic_conf.Precision] {
			return fmt.Errorf("InfluxDB precision must be one of 's', 'ms', 'u', 'ns', 'm', 'h'")
		}
	}

	return nil
}

func (conf *GConfig) String() string {
	return conf.export()
}

func (conf *GConfig) getInfluxConfig(topic string, admin bool) (influx.HTTPConfig, error) {
	return conf.getInfluxConfigByTopicConf(conf.getTopicConf(topic), admin)
}

func (conf *GConfig) getInfluxConfigByTopicConf(topic_conf TopicConf, admin bool) (influx.HTTPConfig, error) {
	var tlsConfigPtr *tls.Config = nil
	var err error

	if topic_conf.TLS.Enable {
		tlsConfigPtr, err = GetTLSConfig(
			topic_conf.TLS.Certificate,
			topic_conf.TLS.PrivateKey,
			topic_conf.TLS.CertificateAuthority,
			topic_conf.TLS.InsecureSkipVerify,
		)
		if err != nil {
			return influx.HTTPConfig{}, errwrap.Wrapf("Failed to understand InfluxDB TLS configuration: {{err}}", err)
		}
	}

	if topic_conf.Auth {
		username := ""
		password := ""
		if admin {
			username = topic_conf.AdminUsername
			password = strings.Replace(topic_conf.AdminPassword, `'`, `\'`, -1)
		} else {
			username = topic_conf.Username
			password = strings.Replace(topic_conf.Password, `'`, `\'`, -1)
		}
		return influx.HTTPConfig{
			Addr:               topic_conf.Host,
			Username:           username,
			Password:           password,
			Timeout:            time.Duration(topic_conf.Timeout) * time.Millisecond,
			InsecureSkipVerify: topic_conf.TLS.InsecureSkipVerify,
			TLSConfig:          tlsConfigPtr,
		}, nil
	}

	return influx.HTTPConfig{
		Addr:               topic_conf.Host,
		Timeout:            time.Duration(topic_conf.Timeout) * time.Millisecond,
		InsecureSkipVerify: topic_conf.TLS.InsecureSkipVerify,
		TLSConfig:          tlsConfigPtr,
	}, nil
}

func (c *GConfig) getSaramaConf() (*sarama.Config, error) {
	var tlsConfigPtr *tls.Config = nil
	var err error
	conf := sarama.NewConfig()

	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Consumer.MaxProcessingTime = 2000 * time.Millisecond
	conf.Consumer.MaxWaitTime = 500 * time.Millisecond
	conf.ClientID = c.Kafka.ClientID
	conf.Version = c.Kafka.cVersion
	conf.ChannelBufferSize = int(2 * c.BatchSize)

	if c.Kafka.TLS.Enable {
		tlsConfigPtr, err = GetTLSConfig(
			c.Kafka.TLS.Certificate,
			c.Kafka.TLS.PrivateKey,
			c.Kafka.TLS.CertificateAuthority,
			c.Kafka.TLS.InsecureSkipVerify,
		)
		if err != nil {
			return nil, errwrap.Wrapf("Failed to understand Kafka TLS configuration: {{err}}", err)
		}
		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = tlsConfigPtr
	}

	if c.Kafka.SASL.Enable {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.Handshake = true
		conf.Net.SASL.User = c.Kafka.SASL.Username
		conf.Net.SASL.Password = c.Kafka.SASL.Password
	}

	return conf, nil
}

func (c *GConfig) getSaramaClusterConf() (*cluster.Config, error) {
	simple_conf, err := c.getSaramaConf()
	if err != nil {
		return nil, err
	}
	cluster_conf := cluster.NewConfig()
	cluster_conf.Config = *simple_conf
	cluster_conf.Group.Return.Notifications = true
	return cluster_conf, nil
}

var cache_topics_confs map[string]TopicConf = map[string]TopicConf{}

func (c *GConfig) cacheTopicsConfs(topics []string) {
	cache_topics_confs = map[string]TopicConf{}
	for _, topic := range topics {
		for _, mapping := range c.Mapping {
			topic_glob := ""
			mapping_name := ""
			for k, v := range mapping {
				topic_glob = k
				mapping_name = v
			}
			g := glob.MustCompile(topic_glob)
			if g.Match(topic) {
				if conf, ok := c.TopicConfs[mapping_name]; ok {
					cache_topics_confs[topic] = conf
				} else {
					cache_topics_confs[topic] = c.TopicConfs["default"]
				}
				break
			}
		}
		if _, ok := cache_topics_confs[topic]; !ok {
			cache_topics_confs[topic] = c.TopicConfs["default"]
		}
	}
}

func (c *GConfig) getTopicConf(topic string) TopicConf {
	if topic_conf, ok := cache_topics_confs[topic]; ok {
		return topic_conf
	}
	return c.TopicConfs["default"]
}

func (c *GConfig) getInfluxClient(topic string) (influx.Client, error) {
	return c.getInfluxClientByTopicConf(c.getTopicConf(topic))
}

func (c *GConfig) getInfluxClientByTopicConf(topic_conf TopicConf) (influx.Client, error) {
	config, err := c.getInfluxConfigByTopicConf(topic_conf, false)
	if err != nil {
		return nil, errwrap.Wrapf("Failed to build configuration for InfluxDB: {{err}}", err)
	}
	client, err := influx.NewHTTPClient(config)
	if err != nil {
		return nil, errwrap.Wrapf("Failed to connect to InfluxDB: {{err}}", err)
	}
	return client, nil
}

func (c *GConfig) getInfluxAdminClient(topic string) (influx.Client, error) {
	return c.getInfluxAdminClientByTopicConf(c.getTopicConf(topic))
}

func (c *GConfig) getInfluxAdminClientByTopicConf(topic_conf TopicConf) (influx.Client, error) {
	config, err := c.getInfluxConfigByTopicConf(topic_conf, true)
	if err != nil {
		return nil, errwrap.Wrapf("Failed to build configuration for InfluxDB: {{err}}", err)
	}
	client, err := influx.NewHTTPClient(config)
	if err != nil {
		return nil, errwrap.Wrapf("Failed to connect to InfluxDB: {{err}}", err)
	}
	return client, nil
}

func (c *GConfig) export() string {
	buf := new(bytes.Buffer)
	encoder := toml.NewEncoder(buf)
	encoder.Encode(*c)
	return buf.String()
}

func set_defaults(v *viper.Viper) {
	v.SetDefault("retry_delay_ms", 30000)
	v.SetDefault("logformat", "text")
	v.SetDefault("batch_size", 5000)
	v.SetDefault("batch_max_duration", 60000)
	v.SetDefault("topics", []string{"metrics_*"})
	v.SetDefault("refresh_topics", 300000)

	v.SetDefault("kafka.brokers", []string{"kafka1", "kafka2", "kafka3"})
	v.SetDefault("kafka.client_id", "kafka2influxdb")
	v.SetDefault("kafka.consumer_group", "kafka2influxdb-cg")
	v.SetDefault("kafka.version", "0.9.0.0")

	v.SetDefault("kafka.tls.enable", false)
	v.SetDefault("kafka.tls.certificate_authority", "")
	v.SetDefault("kafka.tls.certificate", "")
	v.SetDefault("kafka.tls.private_key", "")
	v.SetDefault("kafka.tls.insecure", false)

	v.SetDefault("kafka.sasl.enable", false)
	v.SetDefault("kafka.sasl.username", "")
	v.SetDefault("kafka.sasl.password", "")

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

	v.SetDefault("topic_conf.default.tls.enable", false)
	v.SetDefault("topic_conf.default.tls.certificate_authority", "")
	v.SetDefault("topic_conf.default.tls.certificate", "")
	v.SetDefault("topic_conf.default.tls.private_key", "")
	v.SetDefault("topic_conf.default.tls.insecure", false)

	default_mapping := map[string]string{"*": "default"}
	v.SetDefault("mapping", []map[string]string{default_mapping})
}

func loadConfiguration(dirname string) (*GConfig, error) {
	v := viper.New()
	set_defaults(v)
	v.SetConfigName("kafka2influxdb")
	v.AddConfigPath(dirname)
	v.AddConfigPath("/etc/kafka2influxdb")
	v.AddConfigPath("/usr/local/etc/kafka2influxdb")

	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}

	c := NewConfig()
	err = v.Unmarshal(c)
	if err != nil {
		return nil, err
	}

	mapping_labels_map := map[string]bool{}
	for _, mapping := range c.Mapping {
		for _, label := range mapping {
			mapping_labels_map[label] = true
		}
	}
	mapping_labels := []string{}
	for label, _ := range mapping_labels_map {
		mapping_labels = append(mapping_labels, label)
	}

	// when some param is not set in some topic conf, copy the values from the default conf
	fields := []string{"host", "auth", "username", "password",
		"create_databases", "admin_username", "admin_password", "precision",
		"retention_policy", "timeout", "dbname", "format", "tls.enable",
		"tls.certificate_authority", "tls.certificate", "tls.private_key",
		"tls.insecure"}

	for _, label := range mapping_labels {
		if label == "default" {
			continue
		}
		for _, field := range fields {
			v.SetDefault(fmt.Sprintf("topic_conf.%s.%s", label, field), v.Get(fmt.Sprintf("topic_conf.default.%s", field)))
		}
	}

	err = v.ReadInConfig()
	if err != nil {
		return nil, err
	}

	c = NewConfig()
	err = v.Unmarshal(c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func defaultConfiguration() *GConfig {
	v := viper.New()
	set_defaults(v)
	c := NewConfig()
	v.Unmarshal(c)
	return c
}
