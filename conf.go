package main

import "strings"
import "fmt"
import "strconv"
import "math"
import vutils "github.com/mcuadros/go-version"
import "github.com/Shopify/sarama"

type GConfig struct {
	BatchSize uint32 `toml:"batch_size"`
	Topics    string
	Influxdb  InfluxdbConf
	Databases map[string]string
	Kafka     KafkaConf
}

type InfluxdbConf struct {
	Host            string
	Auth            bool
	Username        string
	Password        string
	Precision       string
	RetentionPolicy string
	Timeout         uint32
	TLS             InfluxTLSConf
}

type InfluxTLSConf struct {
	Enable               bool
	CertificateAuthority string `toml:"certificate_authority"`
	Certificate          string
	PrivateKey           string `toml:"private_key"`
	InsecureSkipVerify 	 bool   `toml:"insecure"`
}

type KafkaConf struct {
	Brokers       []string
	ClientID      string `toml:"client_id"`
	ConsumerGroup string `tml:"consumer_group"`
	Version       string
	cVersion      sarama.KafkaVersion
	TLS           KafkaTLSConf
	SASL          KafkaSASLConf
	Format        string
}

type KafkaTLSConf struct {
	Enable               bool
	CertificateAuthority string `toml:"certificate_authority"`
	Certificate          string
	PrivateKey           string `toml:"private_key"`
	InsecureSkipVerify 	 bool   `toml:"insecure"`
}

type KafkaSASLConf struct {
	Enable   bool
	Username string
	Password string
}

func normalize(s string) string {
	return strings.Trim(strings.ToLower(s), " ")
}

func (conf *GConfig) check() error {

	if conf.BatchSize == 0 {
		conf.BatchSize = 5000
	}

	if conf.Influxdb.Timeout == 0 {
		conf.Influxdb.Timeout = 5000
	}

	if len(conf.Influxdb.Precision) == 0 {
		conf.Influxdb.Precision = "ns"
	}
	conf.Influxdb.Precision = normalize(conf.Influxdb.Precision)

	if len(conf.Kafka.ClientID) == 0 {
		conf.Kafka.ClientID = "kafka2influx"
	}
	if len(conf.Kafka.ConsumerGroup) == 0 {
		conf.Kafka.ConsumerGroup = "kafka2influx-cg"
	}

	if len(conf.Kafka.Version) == 0 {
		conf.Kafka.Version = "0.8.2"
	}

	if len(conf.Kafka.Format) == 0 {
		conf.Kafka.Format = "json"
	}
	conf.Kafka.Format = normalize(conf.Kafka.Format)

	numbers_s := strings.Split(conf.Kafka.Version, ".")
	for _, number_s := range numbers_s {
		_, err := strconv.ParseUint(number_s, 10, 8)
		if err != nil {
			return fmt.Errorf("Kafka version has improper format")
		}
	}

	if vutils.CompareSimple(conf.Kafka.Version, "0.10.1.0") >= 0 {
		conf.Kafka.cVersion = sarama.V0_10_1_0
	} else if vutils.CompareSimple(conf.Kafka.Version, "0.10.0.1") >= 0 {
		conf.Kafka.cVersion = sarama.V0_10_0_1
	} else if vutils.CompareSimple(conf.Kafka.Version, "0.10.0.0") >= 0 {
		conf.Kafka.cVersion = sarama.V0_10_0_0
	} else if vutils.CompareSimple(conf.Kafka.Version, "0.9.0.1") >= 0 {
		conf.Kafka.cVersion = sarama.V0_9_0_1
	} else if vutils.CompareSimple(conf.Kafka.Version, "0.9.0.0") >= 0 {
		conf.Kafka.cVersion = sarama.V0_9_0_0
	} else {
		return fmt.Errorf("Kafka is not recent enough. Needs at least 0.9")
	}

	if !(conf.Kafka.Format == "json" || conf.Kafka.Format == "influx") {
		return fmt.Errorf("Kafka format must be 'influx' or 'json'")
	}

	if len(conf.Topics) == 0 {
		return fmt.Errorf("Provide a glob for kafka topics")
	}

	if len(conf.Kafka.Brokers) == 0 {
		return fmt.Errorf("Provide a list of Kafka brokers")
	}

	if _, ok := conf.Databases["default"]; !ok {
		return fmt.Errorf("Provide a default InfluxDB database")
	}

	if len(conf.Databases["default"]) == 0 {
		return fmt.Errorf("Provide a default InfluxDB database")
	}

	if conf.BatchSize > math.MaxInt32 {
		return fmt.Errorf("BatchSize %d is too big. Max = %d", conf.BatchSize, math.MaxInt32)
	}
	if conf.Influxdb.Auth && (len(conf.Influxdb.Username) == 0 || len(conf.Influxdb.Password) == 0) {
		return fmt.Errorf("InfluxDB authentication is requested but username or password is empty")
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
	s := ""
	s += fmt.Sprintf("Batch size: %d\n", conf.BatchSize)
	s += "\nInfluxDB\n========\n"
	s += fmt.Sprintf("InfluxDB host: %s\n", conf.Influxdb.Host)
	s += fmt.Sprintf("InfluxDB precision: %s\n", conf.Influxdb.Precision)
	s += fmt.Sprintf("InfluxDB with authentication: %t\n", conf.Influxdb.Auth)
	if conf.Influxdb.Auth {
		s += fmt.Sprintf("InfluxDB username: %s\n", conf.Influxdb.Username)
		s += fmt.Sprintf("InfluxDB password: %s\n", conf.Influxdb.Password)
	}
	s += "\nKafka\n=====\n"
	s += fmt.Sprintf("Topics glob: %s\n", conf.Topics)
	s += fmt.Sprintf("Kafka brokers: %s\n", strings.Join(conf.Kafka.Brokers, ", "))
	s += fmt.Sprintf("Kafka client ID: %s\n", conf.Kafka.ClientID)
	s += fmt.Sprintf("Kafka consumer group: %s\n", conf.Kafka.ConsumerGroup)
	s += fmt.Sprintf("Kafka messages format: %s\n", conf.Kafka.Format)
	s += fmt.Sprintf("Kafka Version: %s\n", conf.Kafka.Version)
	s += "\nTopics => Database\n==================\n"
	for topic, dbname := range conf.Databases {
		s += fmt.Sprintf("%s => %s\n", topic, dbname)
	}
	return s
}
