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
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/errwrap"
	"github.com/spf13/viper"
)

type GConfig struct {
	BatchSize        uint32              `mapstructure:"batch_size" toml:"batch_size"`
	BatchMaxDuration uint32              `mapstructure:"batch_max_duration" toml:"batch_max_duration"`
	Topics           []string            `mapstructure:"topics" toml:"topics"`
	RefreshTopics    uint32              `mapstructure:"refresh_topics" toml:"refresh_topics"`
	Influxdb         InfluxdbConf        `mapstructure:"influxdb" toml:"influxdb"`
	Mapping          []map[string]string `mapstructure:"mapping" toml:"mapping"`
	Kafka            KafkaConf           `mapstructure:"kafka" toml:"kafka"`
}

type InfluxdbConf struct {
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
	Format        string        `mapstructure:"format" toml:"format"`
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

var influxdb_default_conf InfluxdbConf = InfluxdbConf{
	Host:            "http://influxdb_host:8086",
	Auth:            false,
	Username:        "",
	Password:        "",
	AdminUsername:   "",
	AdminPassword:   "",
	CreateDatabases: false,
	Precision:       "ns",
	RetentionPolicy: "",
	Timeout:         5000,
	TLS: InfluxTLSConf{
		Enable:               false,
		Certificate:          "",
		CertificateAuthority: "",
		PrivateKey:           "",
		InsecureSkipVerify:   false,
	},
}

var kafka_default_conf KafkaConf = KafkaConf{
	Brokers:       []string{"kafka1", "kafka2", "kafka3"},
	ClientID:      "kafka2influxdb",
	ConsumerGroup: "kafka2influxdb-cg",
	Version:       "0.9.0.0",
	Format:        "json",
	TLS: KafkaTLSConf{
		Enable:               false,
		Certificate:          "",
		CertificateAuthority: "",
		PrivateKey:           "",
		InsecureSkipVerify:   false,
	},
	SASL: KafkaSASLConf{
		Enable:   false,
		Username: "",
		Password: "",
	},
}

var default_mapping map[string]string = map[string]string{"*": "default_db"}

var DefaultConf GConfig = GConfig{
	BatchMaxDuration: 60000,
	BatchSize:        5000,
	Topics:           []string{"metrics_*"},
	RefreshTopics:    300000,
	Mapping:          []map[string]string{default_mapping},
	Influxdb:         influxdb_default_conf,
	Kafka:            kafka_default_conf,
}

func normalize(s string) string {
	return strings.Trim(strings.ToLower(s), " ")
}

func (conf *GConfig) check() error {

	if conf.Influxdb.Auth {
		if len(conf.Influxdb.Username) == 0 {
			conf.Influxdb.Username = conf.Influxdb.AdminUsername
			conf.Influxdb.Password = conf.Influxdb.AdminPassword
		}
		if len(conf.Influxdb.AdminUsername) == 0 {
			conf.Influxdb.AdminUsername = conf.Influxdb.Username
			conf.Influxdb.AdminPassword = conf.Influxdb.Password
		}
	}

	conf.Kafka.Format = normalize(conf.Kafka.Format)
	conf.Kafka.ClientID = strings.Trim(conf.Kafka.ClientID, " ")
	conf.Kafka.ConsumerGroup = strings.Trim(conf.Kafka.ConsumerGroup, " ")
	conf.Kafka.Version = strings.Trim(conf.Kafka.Version, " ")
	conf.Kafka.SASL.Username = strings.Trim(conf.Kafka.SASL.Username, " ")
	conf.Influxdb.Host = strings.Trim(conf.Influxdb.Host, " ")
	conf.Influxdb.Precision = normalize(conf.Influxdb.Precision)
	conf.Influxdb.Username = strings.Trim(conf.Influxdb.Username, " ")
	conf.Influxdb.AdminUsername = strings.Trim(conf.Influxdb.AdminUsername, " ")
	conf.Influxdb.RetentionPolicy = strings.Trim(conf.Influxdb.RetentionPolicy, " ")

	numbers_s := strings.Split(conf.Kafka.Version, ".")
	for _, number_s := range numbers_s {
		_, err := strconv.ParseUint(number_s, 10, 8)
		if err != nil {
			return fmt.Errorf("Kafka version has improper format: %s", conf.Kafka.Version)
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

	if !(conf.Kafka.Format == "json" || conf.Kafka.Format == "influx") {
		return fmt.Errorf("Kafka format must be 'influx' or 'json'")
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
	if conf.Influxdb.Auth {
		if len(conf.Influxdb.Username) == 0 || len(conf.Influxdb.Password) == 0 {
			return fmt.Errorf("InfluxDB authentication is requested but username or password is empty")
		}
		if conf.Influxdb.CreateDatabases && (len(conf.Influxdb.AdminUsername) == 0 || len(conf.Influxdb.AdminPassword) == 0) {
			return fmt.Errorf("InfluxDB authentication is requested, should create InfluxDB databases, but admin username or password is empty")
		}
	}
	if !strings.HasPrefix(conf.Influxdb.Host, "http://") {
		return fmt.Errorf("Incorrect format for InfluxDB host")
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
	if !valid_precisions[conf.Influxdb.Precision] {
		return fmt.Errorf("InfluxDB precision must be one of 's', 'ms', 'u', 'ns', 'm', 'h'")
	}

	return nil
}

func (conf *GConfig) String() string {
	return conf.export()
}

func (conf *GConfig) getInfluxHTTPConfig(admin bool) (influx.HTTPConfig, error) {
	var tlsConfigPtr *tls.Config = nil
	var err error

	if conf.Influxdb.TLS.Enable {
		tlsConfigPtr, err = GetTLSConfig(
			conf.Influxdb.TLS.Certificate,
			conf.Influxdb.TLS.PrivateKey,
			conf.Influxdb.TLS.CertificateAuthority,
			conf.Influxdb.TLS.InsecureSkipVerify,
		)
		if err != nil {
			return influx.HTTPConfig{}, errwrap.Wrapf("Failed to understand InfluxDB TLS configuration: {{err}}", err)
		}
	}

	if conf.Influxdb.Auth {
		username := ""
		password := ""
		if admin {
			username = conf.Influxdb.AdminUsername
			password = strings.Replace(conf.Influxdb.AdminPassword, `'`, `\'`, -1)
		} else {
			username = conf.Influxdb.Username
			password = strings.Replace(conf.Influxdb.Password, `'`, `\'`, -1)
		}
		return influx.HTTPConfig{
			Addr:               conf.Influxdb.Host,
			Username:           username,
			Password:           password,
			Timeout:            time.Duration(conf.Influxdb.Timeout) * time.Millisecond,
			InsecureSkipVerify: conf.Influxdb.TLS.InsecureSkipVerify,
			TLSConfig:          tlsConfigPtr,
		}, nil
	}

	return influx.HTTPConfig{
		Addr:               conf.Influxdb.Host,
		Timeout:            time.Duration(conf.Influxdb.Timeout) * time.Millisecond,
		InsecureSkipVerify: conf.Influxdb.TLS.InsecureSkipVerify,
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
	conf.ChannelBufferSize = 2 * int(c.BatchSize)

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

var cache_topic2_dbname map[string]string = map[string]string{}

func (c *GConfig) listTargetDatabases(topics []string) []string {
	dbnames_map := map[string]bool{}
	for _, topic := range topics {
		dbnames_map[c.topic2dbname(topic)] = true
	}
	dbnames := []string{}
	for dbname, _ := range dbnames_map {
		dbnames = append(dbnames, dbname)
	}
	return dbnames
}

func (c *GConfig) topic2dbname(topic string) string {

	if dbname, ok := cache_topic2_dbname[topic]; ok {
		return dbname
	}
	for _, mapping := range c.Mapping {
		topic_glob := ""
		dbname := ""
		for k, v := range mapping {
			topic_glob = k
			dbname = v
		}

		g := glob.MustCompile(topic_glob)
		if g.Match(topic) {
			if len(dbname) > 0 {
				cache_topic2_dbname[topic] = dbname
				return dbname
			}
			cache_topic2_dbname[topic] = topic
			return topic
		}
	}
	cache_topic2_dbname[topic] = topic
	return topic
}

func (c *GConfig) export() string {
	buf := new(bytes.Buffer)
	encoder := toml.NewEncoder(buf)
	encoder.Encode(*c)
	return buf.String()
}

func ReadConfig(directory string) (*GConfig, error) {
	var config GConfig = DefaultConf
	viper.SetConfigName("kafka2influxdb")
	viper.AddConfigPath(directory)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}
