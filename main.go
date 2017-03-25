package main

import (
	"fmt"
	"log/syslog"
	"os"

	"github.com/Sirupsen/logrus"
	logrus_syslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/alecthomas/kingpin"
)

var log *logrus.Logger

func init() {
	log = logrus.New()
}

var (
	kapp             = kingpin.New("kafka2influxdb", "Get metrics from Kafka and push them to InfluxDB")
	config_fname     = kapp.Flag("config", "configuration filename").Default("/etc/kafka2influx/kafka2influx.toml").String()
	syslog_flag      = kapp.Flag("syslog", "send logs to local syslog").Default("false").Bool()
	check_topics_cmd = kapp.Command("check-topics", "Print which topics in Kafka will be pulled")
	default_conf_cmd = kapp.Command("default-config", "print default configuration")
	check_conf_cmd   = kapp.Command("check-config", "check configuration")
	ping_influx_cmd  = kapp.Command("ping-influxdb", "check connection to influxdb")
	start_cmd        = kapp.Command("start", "start influx2kafka")
)

func main() {
	cmd := kingpin.MustParse(kapp.Parse(os.Args[1:]))

	switch cmd {

	case ping_influx_cmd.FullCommand():
		config_ptr, err := ReadConfig(*config_fname)
		if err != nil {
			log.WithError(err).Fatal("Failed to read configuration file")
		}
		err = config_ptr.check()
		if err != nil {
			log.WithError(err).Fatal("Incorrect configuration")
		}
		app := Kafka2InfluxdbApp{conf: config_ptr}
		version, dbnames, users, err := app.pingInfluxDB()
		if err != nil {
			log.WithError(err).Fatal("Ping InfluxDB failed")
		}
		fmt.Printf("InfluxDB version %s\n", version)
		fmt.Println("\nExisting databases:")
		for _, dbname := range dbnames {
			fmt.Printf("- %s\n", dbname)
		}
		fmt.Println("\nExisting users:")
		for _, user := range users {
			fmt.Printf("- %s\n", user)
		}
	case start_cmd.FullCommand():
		config_ptr, err := ReadConfig(*config_fname)
		if err != nil {
			log.WithField("error", err).Fatal("Failed to read configuration file")
		}
		err = config_ptr.check()
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

		app := Kafka2InfluxdbApp{conf: config_ptr}

		_, _, _, err = app.pingInfluxDB()
		if err != nil {
			log.WithError(err).Fatal("Ping InfluxDB failed")
		}

		var total_count uint64 = 0
		var count uint64 = 0
		restart := true

		for restart {
			count, err, restart = app.consume()
			total_count += count
			count = 0
			if err != nil {
				restart = false
				log.WithError(err).Error("Error while consuming")
			}
		}

		log.Info("Shutting down")
		log.WithField("total_count", total_count).Info("Total number of points fetched from Kafka")

	case check_topics_cmd.FullCommand():
		config_ptr, err := ReadConfig(*config_fname)
		if err != nil {
			log.WithField("error", err).Fatal("Failed to read configuration file")
		}
		err = config_ptr.check()
		if err != nil {
			log.WithError(err).Fatal("Incorrect configuration")
		}

		app := Kafka2InfluxdbApp{conf: config_ptr}
		topics, err := app.getSourceKafkaTopics()
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

	case default_conf_cmd.FullCommand():
		fmt.Println(DefaultConf.export())

	case check_conf_cmd.FullCommand():
		config_ptr, err := ReadConfig(*config_fname)
		if err != nil {
			log.WithField("error", err).Fatal("Failed to read configuration file")
		}
		err = config_ptr.check()
		if err != nil {
			log.WithError(err).Fatal("Incorrect configuration")
		}

		fmt.Println("Configuration looks OK\n")
		fmt.Println(config_ptr.export())

	}
}
