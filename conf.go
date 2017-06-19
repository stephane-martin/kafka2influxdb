package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"errors"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/fsnotify/fsnotify"
	"github.com/gobwas/glob"
	consul_api "github.com/hashicorp/consul/api"
	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/go-version"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/spf13/viper"
)

func NewConfig() *GConfig {
	kafka_conf := KafkaConf{Brokers: []string{}}
	return &GConfig{
		Topics:     []string{},
		Mapping:    []map[string]string{},
		Kafka:      kafka_conf,
		MetricsConf:    MetricsConf{},
		TopicConfs: map[string]TopicConf{},
	}
}

type GConfig struct {
	RetryDelay       uint32               `mapstructure:"retry_delay_ms" toml:"retry_delay_ms"`
	BatchSize        uint32               `mapstructure:"batch_size" toml:"batch_size"`
	BatchMaxDuration uint32               `mapstructure:"batch_max_duration" toml:"batch_max_duration"`
	Topics           []string             `mapstructure:"topics" toml:"topics"`
	RefreshTopics    uint32               `mapstructure:"refresh_topics" toml:"refresh_topics"`
	Mapping          []map[string]string  `mapstructure:"mapping" toml:"mapping"`
	Kafka            KafkaConf            `mapstructure:"kafka" toml:"kafka"`
	TopicConfs       map[string]TopicConf `mapstructure:"topic_conf" toml:"topic_conf"`
	ConfigFilename   string               `toml:"-"`
	MetricsConf	 MetricsConf	      `mapstructure:"metrics" toml:"metrics"`
}

type MetricsConf struct {
	Host                 string `mapstructure:"host" toml:"host"`
	Auth                 bool   `mapstructure:"auth" toml:"auth"`
	Username             string `mapstructure:"username" toml:"username"`
	Password             string `mapstructure:"password" toml:"password"`
	CreateDatabases      bool   `mapstructure:"create_databases" toml:"create_databases"`
	AdminUsername        string `mapstructure:"admin_username" toml:"admin_username"`
	AdminPassword        string `mapstructure:"admin_password" toml:"admin_password"`
	Precision            string `mapstructure:"precision" toml:"precision"`
	RetentionPolicy      string `mapstructure:"retention_policy" toml:"retention_policy"`
	Timeout              uint32 `mapstructure:"timeout" toml:"timeout"`
	DatabaseName         string `mapstructure:"dbname" toml:"dbname"`
	TlsEnable            bool   `mapstructure:"tls_enable" toml:"tls_enable"`
	CertificateAuthority string `mapstructure:"certificate_authority" toml:"certificate_authority"`
	Certificate          string `mapstructure:"certificate" toml:"certificate"`
	PrivateKey           string `mapstructure:"private_key" toml:"private_key"`
	InsecureSkipVerify   bool   `mapstructure:"insecure" toml:"insecure"`
	Enabled		     bool   `mapstructure:"enabled" toml:"enabled"`
	FlushInterval        uint32 `mapstructure:"flush_interval" toml:"flush_interval"`
	Tags		     map[string]string `mapstructure:"tags" toml:"tags"`
}

type TopicConf struct {
	Host                 string `mapstructure:"host" toml:"host"`
	Auth                 bool   `mapstructure:"auth" toml:"auth"`
	Username             string `mapstructure:"username" toml:"username"`
	Password             string `mapstructure:"password" toml:"password"`
	CreateDatabases      bool   `mapstructure:"create_databases" toml:"create_databases"`
	AdminUsername        string `mapstructure:"admin_username" toml:"admin_username"`
	AdminPassword        string `mapstructure:"admin_password" toml:"admin_password"`
	Precision            string `mapstructure:"precision" toml:"precision"`
	RetentionPolicy      string `mapstructure:"retention_policy" toml:"retention_policy"`
	Timeout              uint32 `mapstructure:"timeout" toml:"timeout"`
	DatabaseName         string `mapstructure:"dbname" toml:"dbname"`
	Format               string `mapstructure:"format" toml:"format"`
	TlsEnable            bool   `mapstructure:"tls_enable" toml:"tls_enable"`
	CertificateAuthority string `mapstructure:"certificate_authority" toml:"certificate_authority"`
	Certificate          string `mapstructure:"certificate" toml:"certificate"`
	PrivateKey           string `mapstructure:"private_key" toml:"private_key"`
	InsecureSkipVerify   bool   `mapstructure:"insecure" toml:"insecure"`
}

type KafkaConf struct {
	Brokers              []string            `mapstructure:"brokers" toml:"brokers"`
	ClientID             string              `mapstructure:"client_id" toml:"client_id"`
	ConsumerGroup        string              `mapstructure:"consumer_group" toml:"consumer_group"`
	Version              string              `mapstructure:"version" toml:"version"`
	TlsEnable            bool                `mapstructure:"tls_enable" toml:"tls_enable"`
	CertificateAuthority string              `mapstructure:"certificate_authority" toml:"certificate_authority"`
	Certificate          string              `mapstructure:"certificate" toml:"certificate"`
	PrivateKey           string              `mapstructure:"private_key" toml:"private_key"`
	InsecureSkipVerify   bool                `mapstructure:"insecure" toml:"insecure"`
	SaslEnable           bool                `mapstructure:"sasl_enable" toml:"sasl_enable"`
	SaslUsername         string              `mapstructure:"sasl_username" toml:"sasl_username"`
	SaslPassword         string              `mapstructure:"sasl_password" toml:"sasl_password"`
	cVersion             sarama.KafkaVersion `toml:"-"`
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

	// todo: sanitize brokers
	conf.Kafka.ClientID = strings.Trim(conf.Kafka.ClientID, " ")
	conf.Kafka.ConsumerGroup = strings.Trim(conf.Kafka.ConsumerGroup, " ")
	conf.Kafka.Version = strings.Trim(conf.Kafka.Version, " ")
	conf.Kafka.SaslUsername = strings.Trim(conf.Kafka.SaslUsername, " ")

	if conf.MetricsConf.Auth {
		if len(conf.MetricsConf.Username) == 0 {
			conf.MetricsConf.Username = conf.MetricsConf.AdminUsername
			conf.MetricsConf.Password = conf.MetricsConf.AdminPassword
		}
		if len(conf.MetricsConf.AdminUsername) == 0 {
			conf.MetricsConf.AdminUsername = conf.MetricsConf.Username
			conf.MetricsConf.AdminPassword = conf.MetricsConf.Password
		}
	}
	conf.MetricsConf.Host = strings.Trim(conf.MetricsConf.Host, " ")
	conf.MetricsConf.Precision = normalize(conf.MetricsConf.Precision)
	conf.MetricsConf.Username = strings.Trim(conf.MetricsConf.Username, " ")
	conf.MetricsConf.AdminUsername = strings.Trim(conf.MetricsConf.AdminUsername, " ")
	conf.MetricsConf.RetentionPolicy = strings.Trim(conf.MetricsConf.RetentionPolicy, " ")
	conf.MetricsConf.DatabaseName = strings.Trim(conf.MetricsConf.DatabaseName, " ")
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

	for t, topic_conf := range conf.TopicConfs {
		if !(topic_conf.Format == "json" || topic_conf.Format == "influx") {
			return fmt.Errorf("Kafka format must be 'influx' or 'json', but is '%s' for topic conf '%s'", topic_conf.Format, t)
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
			return fmt.Errorf("Incorrect format for InfluxDB host '%s' in topic conf '%s'", topic_conf.Host, t)
		}

		if !valid_precisions[topic_conf.Precision] {
			return fmt.Errorf("InfluxDB precision must be one of 's', 'ms', 'u', 'ns', 'm', 'h'")
		}
	}
	if conf.MetricsConf.Auth {
		if len(conf.MetricsConf.Username) == 0 || len(conf.MetricsConf.Password) == 0 {
			return fmt.Errorf("InfluxDB authentication is requested but username or password is empty")
		}
		if conf.MetricsConf.CreateDatabases && (len(conf.MetricsConf.AdminUsername) == 0 || len(conf.MetricsConf.AdminPassword) == 0) {
			return fmt.Errorf("InfluxDB authentication is requested, should create InfluxDB databases, but admin username or password is empty")
		}
	}
	if !strings.HasPrefix(conf.MetricsConf.Host, "http://") {
		return fmt.Errorf("Incorrect format for InfluxDB host")
	}

	if !valid_precisions[conf.MetricsConf.Precision] {
		return fmt.Errorf("InfluxDB precision must be one of 's', 'ms', 'u', 'ns', 'm', 'h'")
	}

	return nil
}

func (conf *GConfig) String() string {
	return conf.export()
}

func (conf *GConfig) getInfluxConfig(topic string, admin bool) (influx.HTTPConfig, error) {
	return conf.getInfluxConfigByTopicConf(conf.getTopicConf(topic), admin)
}

func (conf *GConfig) getInfluxConfigByTopicConf(topic_conf *TopicConf, admin bool) (influx.HTTPConfig, error) {
	var tlsConfigPtr *tls.Config = nil
	var err error

	if topic_conf == nil {
		return influx.HTTPConfig{}, fmt.Errorf("nil *TopicConf. That should not happen.")
	}

	if topic_conf.TlsEnable {
		tlsConfigPtr, err = GetTLSConfig(
			topic_conf.Certificate,
			topic_conf.PrivateKey,
			topic_conf.CertificateAuthority,
			topic_conf.InsecureSkipVerify,
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
			InsecureSkipVerify: topic_conf.InsecureSkipVerify,
			TLSConfig:          tlsConfigPtr,
		}, nil
	}

	return influx.HTTPConfig{
		Addr:               topic_conf.Host,
		Timeout:            time.Duration(topic_conf.Timeout) * time.Millisecond,
		InsecureSkipVerify: topic_conf.InsecureSkipVerify,
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

	if c.Kafka.TlsEnable {
		tlsConfigPtr, err = GetTLSConfig(
			c.Kafka.Certificate,
			c.Kafka.PrivateKey,
			c.Kafka.CertificateAuthority,
			c.Kafka.InsecureSkipVerify,
		)
		if err != nil {
			return nil, errwrap.Wrapf("Failed to understand Kafka TLS configuration: {{err}}", err)
		}
		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = tlsConfigPtr
	}

	if c.Kafka.SaslEnable {
		conf.Net.SASL.Enable = true
		conf.Net.SASL.Handshake = true
		conf.Net.SASL.User = c.Kafka.SaslUsername
		conf.Net.SASL.Password = c.Kafka.SaslPassword
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

var cache_topics_confs map[string](*TopicConf) = map[string](*TopicConf){}

func (c *GConfig) cacheTopicsConfs(topics []string) {
	cache_topics_confs = map[string](*TopicConf){}
	for _, topic := range topics {
		// loop on every topic that we should consume from kafka
		for _, mapping := range c.Mapping {
			// loop on every mapping
			topic_glob := ""
			mapping_name := ""
			// a kafka2influxdb mapping is assumed to be just one 'topic glob' -> 'mapping name'
			// the next loop is just a trick to get this
			for k, v := range mapping {
				topic_glob = k
				mapping_name = v
			}
			g := glob.MustCompile(topic_glob)
			if g.Match(topic) {
				// the topic matches the mapping
				if conf, ok := c.TopicConfs[mapping_name]; ok {
					cache_topics_confs[topic] = &conf
				} else {
					// ... but that topic conf does not exist...
					cache_topics_confs[topic] = nil
				}
				break
			}
		}
		// there is no matching mapping for that topic
		if _, ok := cache_topics_confs[topic]; !ok {
			cache_topics_confs[topic] = nil
		}
	}
}

func (c *GConfig) getTopicConf(topic string) *TopicConf {
	if topic_conf, ok := cache_topics_confs[topic]; ok {
		return topic_conf
	}
	return nil
}

func (c *GConfig) getInfluxClient(topic string) (influx.Client, error) {
	return c.getInfluxClientByTopicConf(c.getTopicConf(topic))
}

func (c *GConfig) getInfluxClientByTopicConf(topic_conf *TopicConf) (influx.Client, error) {
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

func (c *GConfig) getInfluxAdminClientByTopicConf(topic_conf *TopicConf) (influx.Client, error) {
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

type ConfigurationWatcher struct {
	filename string
	watcher  *fsnotify.Watcher
	stopping chan bool
}

func NewConfigurationWatcher(filename string) *ConfigurationWatcher {
	c := ConfigurationWatcher{filename: filename}
	return &c
}

func (w *ConfigurationWatcher) Watch() {
	if w.stopping != nil {
		return
	}
	var err error
	w.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.WithError(err).Error("Error happened trying to watch configuration file")
		return
	}

	// StopWatch will close w.stopping. When that happens, Watch() will return. The defer
	// will close the watcher. Closing the watcher closes the event chan, and makes
	// the goroutine end.
	defer w.watcher.Close()
	w.stopping = make(chan bool)

	go func() {
	Loop:
		for {
			select {
			case event, more := <-w.watcher.Events:
				if more {
					if filepath.Clean(event.Name) == w.filename {
						if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
							SighupMyself()
						}
					}
				} else {
					break Loop
				}
			case err, more := <-w.watcher.Errors:
				if more {
					log.WithError(err).Error("Watcher error")
				}
			}
		}
	}()

	w.watcher.Add(filepath.Dir(w.filename))
	<-w.stopping
}

func (w *ConfigurationWatcher) StopWatch() {
	close(w.stopping)
}

func LoadConf(dirname, consul_addr, consul_prefix, consul_token, consul_datacenter string, notify chan bool) (c *GConfig, stop_watch chan bool, err error) {

	defer func() {
		// sometimes viper panics... let's catch that
		if r := recover(); r != nil {
			log.WithField("recover", r).Error("Panic has been recovered in LoadConf")
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown panic")
			}
		}
		if err != nil {
			sclose(stop_watch)
			sclose(notify)
			stop_watch = nil
			c = nil
		}
	}()

	v := viper.New()
	set_defaults(v)
	v.SetConfigName("kafka2influxdb")
	v.AddConfigPath(dirname)
	v.AddConfigPath("/etc/kafka2influxdb")
	v.AddConfigPath("/usr/local/etc/kafka2influxdb")

	err = v.ReadInConfig()
	if err != nil {
		switch err.(type) {
		default:
			err = errwrap.Wrapf("Error reading the configuration file: {{err}}", err)
			return
		case viper.ConfigFileNotFoundError:
		}
	}

	if len(consul_addr) > 0 {
		var cclient *consul_api.Client
		var results map[string]string
		cclient, err = NewConsulClient(consul_addr, consul_token, consul_datacenter)
		if err != nil {
			log.WithError(err).Error("Error creating consul client")
			return
		}
		results, stop_watch, err = WatchTree(cclient, consul_prefix, notify)
		if err != nil {
			log.WithError(err).Error("Error getting configuration items from Consul")
			return
		}
		ParseConfigFromConsul(v, consul_prefix, results)
	} else {
		sclose(notify)
	}

	c = NewConfig()
	err = v.Unmarshal(c)
	if err != nil {
		log.WithError(err).Error("Error unmarshaling configuration")
		return
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
			k := fmt.Sprintf("topic_conf.%s.%s", label, field)
			def_val := v.Get(fmt.Sprintf("topic_conf.default.%s", field))
			v.SetDefault(k, def_val)
		}
	}

	var fname string
	// reread conf with the new defaults
	err = v.ReadInConfig()
	if err == nil {
		fname = filepath.Clean(v.ConfigFileUsed())
	} else {
		switch err.(type) {
		default:
			err = errwrap.Wrapf("Error reading the configuration file", err)
			return
		case viper.ConfigFileNotFoundError:
			log.WithError(err).Debug("No configuration file was found")
		}
	}

	c = NewConfig()
	err = v.Unmarshal(c)
	if err != nil {
		return
	}

	// remove the topic_conf sections that are not used in a mapping
	// this way the `check` method will ignore unused topic configurations
	// that may be invalid
	valid_topic_confs := map[string]TopicConf{}
	for tc_k, tc_v := range c.TopicConfs {
		_, ok := mapping_labels_map[tc_k]
		if ok {
			valid_topic_confs[tc_k] = tc_v
		} else {
			log.WithField("topic_conf", tc_k).Info("Ignoring unused topic_conf section")
		}
	}
	// but do not remove the default conf!
	// https://github.com/stephane-martin/kafka2influxdb/issues/19
	valid_topic_confs["default"] = c.TopicConfs["default"]
	c.TopicConfs = valid_topic_confs

	if len(fname) > 0 {
		c.ConfigFilename = fname
		log.WithField("file", c.ConfigFilename).Debug("Found configuration file")
	}
	return
}

func defaultConfiguration() *GConfig {
	v := viper.New()
	set_defaults(v)
	c := NewConfig()
	v.Unmarshal(c)
	return c
}

func ParseConfigFromConsul(vi *viper.Viper, prefix string, c map[string]string) {
	mappings := []map[string]string{}
	for k, val := range c {
		val = strings.TrimSpace(val)
		k = strings.Trim(k, "\n\r\t /")[len(prefix):]
		k = strings.Trim(k, "\n\r\t /")
		s := strings.Split(k, "/")
		switch s[0] {
		case "mappings":
			lines := strings.Split(val, "\n")
			for _, l := range lines {
				l = strings.Trim(l, "\r\n\t ")
				m := map[string]string{}
				_, err := toml.Decode(l, &m)
				if err == nil {
					mappings = append(mappings, m)
				} else {
					log.WithError(err).WithField("mapping", l).Error("Failed to parse mapping from Consul")
				}
			}
		case "global":
			if s[1] == "topics" {
				topics := []string{}
				for _, topic := range strings.Split(val, ",") {
					topics = append(topics, strings.TrimSpace(topic))
				}
				vi.Set("topics", topics)
			} else {
				k2 := strings.Join(s[1:], ".")
				vi.Set(k2, val)
			}
		case "kafka":
			if s[1] == "brokers" {
				brokers := []string{}
				for _, broker := range strings.Split(val, ",") {
					brokers = append(brokers, strings.TrimSpace(broker))
				}
				vi.Set("kafka.brokers", strings.Split(val, ","))
			} else {
				k2 := strings.Join(s, ".")
				vi.Set(k2, val)
			}
		default:
			k2 := strings.Join(s, ".")
			vi.Set(k2, val)
		}
	}
	if len(mappings) > 0 {
		vi.Set("mapping", mappings)
	}

}

func (conf *GConfig) getMetricsInfluxHTTPClient() (influx.Client, error) {
	influx_conf, err := conf.getInfluxConfigMetricsConf()
	if err != nil {
		log.Error("Failed to generate influx conf from metrics conf")
	}
	client, err := influx.NewHTTPClient(influx_conf)
	if err != nil {
		log.Error("Failed to connect to InfluxDB: {{err}}", err)
	}
	return client, err
}

func (conf *GConfig) getInfluxConfigMetricsConf() (influx.HTTPConfig, error) {
	var tlsConfigPtr *tls.Config = nil
	var err error

	if conf.MetricsConf.TlsEnable {
		tlsConfigPtr, err = GetTLSConfig(
			conf.MetricsConf.Certificate,
			conf.MetricsConf.PrivateKey,
			conf.MetricsConf.CertificateAuthority,
			conf.MetricsConf.InsecureSkipVerify,
		)
		if err != nil {
			return influx.HTTPConfig{}, errwrap.Wrapf("Failed to understand InfluxDB TLS configuration: {{err}}", err)
		}
	}

	if conf.MetricsConf.Auth {
		username := ""
		password := ""
		if false {
			username = conf.MetricsConf.AdminUsername
			password = strings.Replace(conf.MetricsConf.AdminPassword, `'`, `\'`, -1)
		} else {
			username = conf.MetricsConf.Username
			password = strings.Replace(conf.MetricsConf.Password, `'`, `\'`, -1)
		}
		return influx.HTTPConfig{
			Addr:               conf.MetricsConf.Host,
			Username:           username,
			Password:           password,
			Timeout:            time.Duration(conf.MetricsConf.Timeout) * time.Millisecond,
			InsecureSkipVerify: conf.MetricsConf.InsecureSkipVerify,
			TLSConfig:          tlsConfigPtr,
		}, nil
	}

	return influx.HTTPConfig{
		Addr:               conf.MetricsConf.Host,
		Timeout:            time.Duration(conf.MetricsConf.Timeout) * time.Millisecond,
		InsecureSkipVerify: conf.MetricsConf.InsecureSkipVerify,
		TLSConfig:          tlsConfigPtr,
	}, nil
}
