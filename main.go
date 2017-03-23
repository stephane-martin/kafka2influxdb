package main

import (
	"fmt"
	"io/ioutil"
	"log/syslog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	logrus_syslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/alecthomas/kingpin"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gobwas/glob"
	influx "github.com/influxdata/influxdb/client/v2"
)

var log = logrus.New()

var (
	kapp             = kingpin.New("kafka2influxdb", "Get metrics from Kafka and push them to InfluxDB")
	config_fname     = kapp.Flag("config", "configuration filename").Default("/etc/kafka2influx/kafka2influx.toml").String()
	syslog_flag      = kapp.Flag("syslog", "send logs to local syslog").Default("false").Bool()
	check_topics_cmd = kapp.Command("check-topics", "Print which topics in Kafka will be pulled")
	print_conf_cmd   = kapp.Command("print-config", "print configuration")
	check_conf_cmd   = kapp.Command("check-config", "check configuration")
	start_cmd        = kapp.Command("start", "start influx2kafka")
)

type Kafka2InfluxdbApp struct {
	conf *GConfig
}

const (
	BOOLEAN MetricValueType = iota
	STRING
	NUMERIC
)

func (app *Kafka2InfluxdbApp) topic2dbname(topic string) string {
	if dbname, ok := app.conf.Databases[topic]; ok {
		return dbname
	} else {
		return app.conf.Databases["default"]
	}
}

func createDatabase(dbname string, client influx.Client) (err error) {
	q := influx.NewQuery(fmt.Sprintf("CREATE DATABASE %s", dbname), "", "")
	_, err = client.Query(q)
	return err
}

func (app *Kafka2InfluxdbApp) createDatabases(topics []string) (err error) {
	databases_set := map[string]bool{}
	for _, topic := range topics {
		databases_set[app.topic2dbname(topic)] = true
	}
	databases := []string{}
	for dbname, _ := range databases_set {
		databases = append(databases, dbname)
	}

	config, err := app.conf.getInfluxHTTPConfig()
	if err != nil {
		log.WithError(err).
			WithField("action", "creating TLS configuration").
			WithField("function", "createDatabases").
			Error("Error when reading TLS configuration for InfluxDB")
	}

	client, err := influx.NewHTTPClient(config)
	if err != nil {
		log.WithError(err).
			WithField("action", "connecting to influxdb").
			WithField("function", "createDatabases").
			Error("Error connecting to InfluxDB")
		return err
	}
	defer client.Close()
	for _, dbname := range databases {
		if err := createDatabase(dbname, client); err != nil {
			log.WithError(err).
				WithField("action", "creating influxdb database").
				WithField("function", "createDatabases").
				WithField("dbname", dbname).
				Error("Error creating a database in InfluxDB")
			return err
		}
	}
	return nil
}

func (app *Kafka2InfluxdbApp) getTelegrafTopics() ([]string, error) {

	var consumer sarama.Consumer
	selected_topics := []string{}
	topics_map := map[string]bool{}

	sarama_conf, err := app.conf.getSaramaConf()
	if err != nil {
		return selected_topics, err
	}

	client, err := sarama.NewClient(app.conf.Kafka.Brokers, sarama_conf)
	if err != nil {
		log.WithError(err).Error("Error creating the sarama Kafka client")
		return selected_topics, err
	}
	consumer, err = sarama.NewConsumerFromClient(client)
	if err != nil {
		log.WithError(err).Error("Error creating the sarama Kafka consumer")
		return selected_topics, err
	}

	defer func() {
		if consumer != nil {
			if lerr := consumer.Close(); lerr != nil {
				log.WithError(err).Error("Error while closing Kafka consumer")
			}
		}
		if client != nil {
			if lerr := client.Close(); lerr != nil {
				log.WithError(err).Error("Error while closing Kafka client")
			}
		}
	}()

	alltopics, err := consumer.Topics()
	if err != nil {
		return selected_topics, err
	}

	for _, topic_glob := range app.conf.Topics {
		g := glob.MustCompile(topic_glob)

		for _, topic := range alltopics {
			if g.Match(topic) {
				topics_map[topic] = true
			}
		}
	}

	for topic, _ := range topics_map {
		selected_topics = append(selected_topics, topic)
	}

	return selected_topics, nil

}

// {"fields":{"pid":29738,"seafdav_cpu_time_guest":0,"seafdav_cpu_time_guest_nice":0,"seafdav_cpu_time_idle":0,"seafdav_cpu_time_iowait":0,"seafdav_cpu_time_irq":0,"seafdav_cpu_time_nice":0,"seafdav_cpu_time_soft_irq":0,"seafdav_cpu_time_steal":0,"seafdav_cpu_time_stolen":0,"seafdav_cpu_time_system":0.25,"seafdav_cpu_time_user":0.77,"seafdav_involuntary_context_switches":12,"seafdav_memory_rss":17620992,"seafdav_memory_swap":0,"seafdav_memory_vms":427905024,"seafdav_num_threads":6,"seafdav_voluntary_context_switches":13361},"name":"procstat","tags":{"host":"seafile_container","pidfile":"/home/seafile/pids/seafdav.pid","process_name":"python2.7","servertype":"LXC"},"timestamp":1489196511}

func (app *Kafka2InfluxdbApp) process(pack []*sarama.ConsumerMessage) (err error) {
	// todo: kafka source in line protocol format

	config, err := app.conf.getInfluxHTTPConfig()
	if err != nil {
		log.WithError(err).
			WithField("action", "creating TLS configuration").
			WithField("function", "process").
			Error("Error when reading TLS configuration for InfluxDB")
	}

	client, err := influx.NewHTTPClient(config)
	if err != nil {
		log.WithError(err).Error("Error connecting to InfluxDB")
		return err
	}
	defer client.Close()
	log.WithField("nb_points", len(pack)).Info("Pushing points to InfluxDB")
	topicBatchMap := map[string]influx.BatchPoints{}

	var parse_fun func([]byte, string) (*influx.Point, error)
	if app.conf.Kafka.Format == "json" {
		parse_fun = parseJsonPoint
	} else if app.conf.Kafka.Format == "influx" {
		parse_fun = parseLineProtocolPoint
	} else {
		return fmt.Errorf("Unknown format for points in Kafka")
	}

	for _, msg := range pack {
		if _, ok := topicBatchMap[msg.Topic]; !ok {
			bp, _ := influx.NewBatchPoints(
				influx.BatchPointsConfig{
					Database:        app.topic2dbname(msg.Topic),
					Precision:       app.conf.Influxdb.Precision,
					RetentionPolicy: app.conf.Influxdb.RetentionPolicy,
				},
			)
			topicBatchMap[msg.Topic] = bp
		}
		point, err := parse_fun(msg.Value, app.conf.Influxdb.Precision)
		if err == nil {
			if point != nil {
				topicBatchMap[msg.Topic].AddPoint(point)
			}
		} else {
			log.WithError(err).
				WithField("message", msg.Value).
				Error("error happened when parsing a point from JSON")
		}
	}
	for topic, bp := range topicBatchMap {
		dbname := bp.Database()
		l := len(bp.Points())

		if l > 0 {
			log.WithField("nb_points", l).
				WithField("database", dbname).
				WithField("topic", topic).
				Info("Writing points to InfluxDB")
			err := client.Write(bp)
			if err != nil {
				log.WithError(err).
					WithField("database", dbname).
					WithField("topic", topic).
					Error("Error happened when writing points to InfluxDB")
				return err
			}
		}
	}
	return nil
}

func (app *Kafka2InfluxdbApp) processAndCommit(consumer *cluster.Consumer, pack *[]*sarama.ConsumerMessage) error {
	err := app.process(*pack)
	if err != nil {
		return err
	}

	for _, m := range *pack {
		consumer.MarkOffset(m, "")
	}
	consumer.CommitOffsets()

	*pack = []*sarama.ConsumerMessage{}

	return nil
}

func (app *Kafka2InfluxdbApp) consume() (uint64, error) {

	sarama_conf, _ := app.conf.getSaramaClusterConf()
	var count uint32
	var total_count uint64

	topics, err := app.getTelegrafTopics()

	if err != nil {
		return 0, err
	}

	if len(topics) == 0 {
		log.Warn("No Kafka topic is matching your glob")
		return 0, nil
	}

	if err = app.createDatabases(topics); err != nil {
		return 0, err
	}

	consumer, err := cluster.NewConsumer(app.conf.Kafka.Brokers, app.conf.Kafka.ConsumerGroup, topics, sarama_conf)
	if err != nil {
		log.WithError(err).Error("Error creating the sarama Kafka consumer")
		return 0, err
	}
	defer consumer.Close()

	log.WithField("topics", strings.Join(topics, ",")).Info("Consuming from these fields")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	pack_of_messages := []*sarama.ConsumerMessage{}

	for {
		select {
		case <-signals:
			return total_count, nil
		default:
			select {
			case msg, more := <-consumer.Messages():
				if more {
					count++
					total_count++
					pack_of_messages = append(pack_of_messages, msg)
					if count >= app.conf.BatchSize {
						count = 0
						err := app.processAndCommit(consumer, &pack_of_messages)
						if err != nil {
							return total_count, err
						}
					}
				}
			case err, more := <-consumer.Errors():
				if more {
					log.WithError(err).Error("Error with Kafka consumer")
				}
			case ntf, more := <-consumer.Notifications():
				if more {
					log.WithField("ntf", fmt.Sprintf("%+v", ntf)).Info("Rebalanced")
				}
			case <-time.After(time.Second * 1):
				log.Debug("No new message from kafka in the last second")
				if len(pack_of_messages) > 0 {
					count = 0
					err := app.processAndCommit(consumer, &pack_of_messages)
					if err != nil {
						return total_count, err
					}
				}
			}
		}
	}
}

func getConfig(filename string) (*GConfig, error) {
	var config GConfig
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(b) {
		return nil, fmt.Errorf("%s is not a properly utf-8 encoded file", filename)
	}
	_, parse_err := toml.Decode(string(b), &config)
	if parse_err != nil {
		return nil, parse_err
	}
	return &config, nil
}

func main() {
	cmd := kingpin.MustParse(kapp.Parse(os.Args[1:]))
	config_ptr, err := getConfig(*config_fname)
	if err != nil {
		log.WithField("error", err).Fatal("Failed to read configuration file")
	}

	app := Kafka2InfluxdbApp{conf: config_ptr}

	switch cmd {

	case start_cmd.FullCommand():
		err = app.conf.check()
		if err != nil {
			log.WithError(err).Fatal("Incorrect configuration")
		}
		if *syslog_flag {
			hook, err := logrus_syslog.NewSyslogHook("", "", syslog.LOG_INFO, "")
			if err == nil {
				log.Formatter = &logrus.TextFormatter{DisableColors: true, DisableTimestamp: true}
				log.Hooks.Add(hook)
			} else {
				log.WithError(err).Error("Unable to connect to local syslog daemon")
			}
		}

		total_count, err := app.consume()
		log.Info("Shutting down")
		log.WithField("total_count", total_count).Info("Total number of points fetched from Kafka")
		if err != nil {
			log.WithError(err).Fatal("Error happened when processing")
		}

	case check_topics_cmd.FullCommand():
		err = app.conf.check()
		if err != nil {
			log.WithError(err).Fatal("Incorrect configuration")
		}
		topics, err := app.getTelegrafTopics()
		if err != nil {
			log.WithError(err).Fatal("Error while fetching topics from Kafka")
		}
		if len(topics) > 0 {
			for _, topic := range topics {
				fmt.Println(topic)
			}
		} else {
			log.Fatal("No topic match your topic glob")
		}

	case print_conf_cmd.FullCommand():
		fmt.Printf("Configuration file: %s\n\n", *config_fname)
		err = app.conf.check()
		if err != nil {
			log.WithError(err).Fatal("Incorrect configuration")
		}
		fmt.Println(app.conf)
		fmt.Println()

	case check_conf_cmd.FullCommand():
		err = app.conf.check()
		if err != nil {
			log.WithError(err).Fatal("Incorrect configuration")
		}
		fmt.Println("OK!")
	}
}
