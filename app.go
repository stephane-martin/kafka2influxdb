package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gobwas/glob"
)

type Kafka2InfluxdbApp struct {
	conf *GConfig
}

func (app *Kafka2InfluxdbApp) createDatabases(topics []string) (err error) {
	databases_set := map[string]bool{}
	for _, topic := range topics {
		databases_set[app.conf.topic2dbname(topic)] = true
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

func (app *Kafka2InfluxdbApp) process(pack []*sarama.ConsumerMessage) (err error) {
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
					Database:        app.conf.topic2dbname(msg.Topic),
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
