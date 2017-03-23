package main

import (
	"fmt"
	"io/ioutil"
	"log/syslog"
	"os"
	"unicode/utf8"

	"github.com/BurntSushi/toml"
	"github.com/Sirupsen/logrus"
	logrus_syslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/alecthomas/kingpin"
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

const (
	BOOLEAN MetricValueType = iota
	STRING
	NUMERIC
)

func createDatabase(dbname string, client influx.Client) (err error) {
	q := influx.NewQuery(fmt.Sprintf("CREATE DATABASE %s", dbname), "", "")
	_, err = client.Query(q)
	return err
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
