package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gobwas/glob"
	"github.com/hashicorp/errwrap"
	influx "github.com/influxdata/influxdb/client/v2"
)

type Kafka2InfluxdbApp struct {
	conf *GConfig
}

func NewApp() *Kafka2InfluxdbApp {
	return &Kafka2InfluxdbApp{conf: NewConfig()}
}

type Message struct {
	raw    *sarama.ConsumerMessage
	parsed *influx.Point
}

func (app *Kafka2InfluxdbApp) reloadConfiguration(dirname string) error {
	// read the configuration file
	new_conf, err := loadConfiguration(dirname)
	if err != nil {
		return errwrap.Wrapf("Failed to read the configuration file: {{err}}", err)
	}
	// check that configuration is OK
	err = new_conf.check()
	if err != nil {
		return errwrap.Wrapf("Incorrect configuration: {{err}}", err)
	}
	app.conf = new_conf
	return nil
}

func listExistingUsers(client influx.Client) ([]string, error) {
	q := influx.NewQuery("SHOW USERS", "", "")
	users := []string{}
	response, err := client.Query(q)
	if err != nil {
		return users, errwrap.Wrapf("SHOW USERS failed: {{err}}", err)
	}
	if response.Error() != nil {
		return users, errwrap.Wrapf("SHOW USERS failed: {{err}}", response.Error())
	}
	vals := response.Results[0].Series[0].Values
	for _, val := range vals {
		users = append(users, val[0].(string))
	}
	return users, nil
}

func listExistingDatabases(client influx.Client) ([]string, error) {
	q := influx.NewQuery("SHOW DATABASES", "", "")
	databases := []string{}
	response, err := client.Query(q)
	if err != nil {
		return databases, errwrap.Wrapf("SHOW DATABASES failed: {{err}}", err)
	}
	if response.Error() != nil {
		return databases, errwrap.Wrapf("SHOW DATABASES failed: {{err}}", response.Error())
	}
	vals := response.Results[0].Series[0].Values
	for _, val := range vals {
		databases = append(databases, val[0].(string))
	}
	return databases, nil
}

func (app *Kafka2InfluxdbApp) pingInfluxDB() (map[string]string, error) {
	versions := map[string]string{}

	for mapping_name, topic_conf := range app.conf.TopicConfs {

		client, err := app.conf.getInfluxAdminClientByTopicConf(topic_conf)

		if err != nil {
			log.WithError(err).Error("Error connecting to InfluxDB")
			return nil, err
		}
		defer client.Close()

		d, version, err := client.Ping(time.Duration(topic_conf.Timeout) * time.Millisecond)
		if err != nil {
			log.WithError(err).WithField("duration", d).Error("Ping failed")
			return nil, err
		}
		versions[mapping_name] = version
	}
	return versions, nil
}

func (app *Kafka2InfluxdbApp) createWriteUser(topic string) error {

	topic_conf := app.conf.getTopicConf(topic)
	if topic_conf.Username == topic_conf.AdminUsername {
		// only one kind of influxdb user was provided
		// if that user is admin, nothing to do (already exists, right ?)
		// if that user is not admin, we don't have an admin account to create it
		// so let's give up
		return nil
	}

	// connecting to influxDB (as admin)
	client, err := app.conf.getInfluxAdminClient(topic)
	if err != nil {
		log.WithError(err).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			WithField("function", "createWriteUser").
			Error("Error connecting to InfluxDB")
		return err
	}
	defer client.Close()

	// list existing users
	existing_users, err := listExistingUsers(client)
	if err != nil {
		// will fail if the admin user is not really admin on influxdb
		log.WithError(err).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			WithField("function", "createWriteUser").
			Error("Error listing users in Influxdb")
		return err
	}

	already_exists := func() bool {
		for _, existing_user := range existing_users {
			if existing_user == topic_conf.Username {
				return true
			}
		}
		return false
	}

	if already_exists() {
		log.WithField("username", topic_conf.Username).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			Info("User already exists in InfluxDB")
	} else {
		// creating new user
		q := influx.NewQuery(
			fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD '%s'`,
				strings.Replace(topic_conf.Username, `"`, `\"`, -1),
				strings.Replace(topic_conf.Password, `'`, `\'`, -1)),
			"", "")
		resp, err := client.Query(q)
		if err != nil {
			log.WithError(err).
				WithField("topic", topic).
				WithField("host", topic_conf.Host).
				WithField("username", topic_conf.Username).
				WithField("function", "createWriteUser").
				Error("Error creating user in InfluxDB")
			return errwrap.Wrapf("Failed to create user in InfluxDB: {{err}}", err)
		} else if resp.Error() != nil {
			log.WithError(resp.Error()).
				WithField("topic", topic).
				WithField("host", topic_conf.Host).
				WithField("username", topic_conf.Username).
				WithField("function", "createWriteUser").
				Error("Error creating user in InfluxDB")
			return errwrap.Wrapf("Failed to create user in InfluxDB: {{err}}", resp.Error())
		} else {
			log.WithField("username", topic_conf.Username).
				WithField("host", topic_conf.Host).
				WithField("topic", topic).
				Info("Created user in InfluxDB")
		}
	}
	return nil
}

func (app *Kafka2InfluxdbApp) createDatabase(topic string) error {
	topic_conf := app.conf.getTopicConf(topic)

	client, err := app.conf.getInfluxAdminClient(topic)
	if err != nil {
		log.WithError(err).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			WithField("function", "createDatabase").
			Error("Error connecting to InfluxDB")
		return err
	}
	defer client.Close()

	dbname := topic_conf.DatabaseName

	// list exiting databases
	existing_databases, err := listExistingDatabases(client)
	if err != nil {
		log.WithError(err).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			WithField("function", "createDatabase").
			Error("Error listing existing databases")
		return err
	}

	db_already_exists := func() bool {
		for _, existing := range existing_databases {
			if existing == dbname {
				return true
			}
		}
		return false
	}

	create_database := func() error {
		q := influx.NewQuery(fmt.Sprintf("CREATE DATABASE %s", dbname), "", "")
		resp, err := client.Query(q)
		if err != nil {
			return errwrap.Wrapf("CREATE DATABASE failed: {{err}}", err)
		} else if resp.Error() != nil {
			return errwrap.Wrapf("CREATE DATABASE failed: {{err}}", resp.Error())
		}
		return nil
	}

	if db_already_exists() {
		log.WithField("database", dbname).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			Info("Database already exists")
	} else {
		err := create_database()
		if err == nil {
			log.WithField("database", dbname).
				WithField("topic", topic).
				WithField("host", topic_conf.Host).
				Info("Created database")
		} else {
			log.WithError(err).
				WithField("action", "creating influxdb database").
				WithField("function", "createDatabase").
				WithField("dbname", dbname).
				WithField("host", topic_conf.Host).
				WithField("topic", topic).
				Error("Error creating a database in InfluxDB")
			return err
		}
	}
	return nil
}

func (app *Kafka2InfluxdbApp) grantDatabaseRights(topic string) {
	// try to grant read and write rights on the target databases to the
	// "normal" influxdb user

	topic_conf := app.conf.getTopicConf(topic)

	client, err := app.conf.getInfluxClient(topic)
	if err != nil {
		log.WithError(err).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			WithField("function", "grantDatabaseRights").
			Error("Error connecting to InfluxDB")
		return
	}
	defer client.Close()

	// only an admin can SHOW DATABASES, so if that query succeeds, the normal
	// user is in fact an admin and we have nothing to do
	q := influx.NewQuery("SHOW DATABASES", "", "")
	resp, err := client.Query(q)
	if err == nil && resp.Error() == nil {
		log.WithField("topic", topic).
			WithField("host", topic_conf.Host).
			Info("InfluxDB normal user is in fact admin, no GRANT needs to be performed")
		return
	}

	// the normal user is not an admin
	log.WithField("topic", topic).
		WithField("host", topic_conf.Host).
		Info("InfluxDB normal user is not an admin")

	// check that normal user and admin user are different
	if topic_conf.Username == topic_conf.AdminUsername {
		log.WithField("topic", topic).
			WithField("host", topic_conf.Host).
			Info("No InfluxDB admin user was provided. Can't GRANT rights.")
		return
	}

	admin_client, err := app.conf.getInfluxAdminClient(topic)
	if err != nil {
		log.WithError(err).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			WithField("function", "grantDatabaseRights").
			Error("Error connecting to InfluxDB")
		return
	}
	defer admin_client.Close()

	// check that the provided InfluxDB user is truely admin
	q = influx.NewQuery("SHOW DATABASES", "", "")
	resp, err = admin_client.Query(q)
	if err != nil || resp.Error() != nil {
		log.WithField("topic", topic).
			WithField("host", topic_conf.Host).
			Warn("The provided InfluxDB admin user does not really have admin rights.")
		return
	}

	dbname := topic_conf.DatabaseName

	// use admin user to grant rights on the InfluxDB target database
	// (the GRANT will succeed even if the target database does not exist)
	// todo: escaping
	q_str := fmt.Sprintf("GRANT ALL ON %s to %s", dbname, topic_conf.Username)
	q = influx.NewQuery(q_str, dbname, "")
	resp, err = admin_client.Query(q)
	if err != nil {
		log.WithError(err).
			WithField("topic", topic).
			WithField("username", topic_conf.Username).
			WithField("host", topic_conf.Host).
			WithField("database", dbname).
			Info("Failed to GRANT rights")
	} else if resp.Error() != nil {
		log.WithError(resp.Error()).
			WithField("topic", topic).
			WithField("username", topic_conf.Username).
			WithField("host", topic_conf.Host).
			WithField("database", dbname).
			Info("Failed to GRANT rights")
	} else {
		log.WithField("database", dbname).
			WithField("topic", topic).
			WithField("host", topic_conf.Host).
			WithField("username", topic_conf.Username).
			Info("GRANTed rights")
	}
}

func (app *Kafka2InfluxdbApp) checkDatabase(topic string) (err error) {
	// check that the target database exists and that the influxdb user can connect to it

	topic_conf := app.conf.getTopicConf(topic)

	client, err := app.conf.getInfluxClient(topic)
	if err != nil {
		log.WithError(err).
			WithField("function", "checkDatabases").
			Error("Error connecting to InfluxDB")
		return err
	}
	defer client.Close()

	dbname := topic_conf.DatabaseName

	// try to connect to the database
	q := influx.NewQuery("SHOW SERIES", dbname, "")
	resp, err := client.Query(q)
	if err != nil || resp.Error() != nil {
		if err == nil {
			err = resp.Error()
		}
		log.WithError(err).
			WithField("database", dbname).
			WithField("function", "checkDatabases").
			Error("Failed to connect")
		return errwrap.Wrapf("Failed to connect to InfluxDB: {{err}}", err)
	}
	return nil
}

func (app *Kafka2InfluxdbApp) getSourceKafkaTopics() ([]string, error) {

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
		return selected_topics, errwrap.Wrapf("Failed to create the Kafka client: {{err}}", err)
	}
	consumer, err = sarama.NewConsumerFromClient(client)
	if err != nil {
		log.WithError(err).Error("Error creating the sarama Kafka consumer")
		return selected_topics, errwrap.Wrapf("Failed to create the Kafka consumer: {{err}}", err)
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
		return selected_topics, errwrap.Wrapf("Failed to retrieve Topics names from Kafka: {{err}}", err)
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

func (app *Kafka2InfluxdbApp) process(pack []Message) (err error) {

	log.WithField("nb_points", len(pack)).Info("Number of points to push to InfluxDB")
	topicBatchMap := map[string]influx.BatchPoints{}

	for _, msg := range pack {
		topic := msg.raw.Topic
		topic_conf := app.conf.getTopicConf(topic)
		if _, ok := topicBatchMap[topic]; !ok {
			bp, _ := influx.NewBatchPoints(
				influx.BatchPointsConfig{
					Database:        topic_conf.DatabaseName,
					Precision:       topic_conf.Precision,
					RetentionPolicy: topic_conf.RetentionPolicy,
				},
			)
			topicBatchMap[topic] = bp
		}
		if msg.parsed != nil {
			topicBatchMap[topic].AddPoint(msg.parsed)
		}
	}
	for topic, bp := range topicBatchMap {

		dbname := bp.Database()
		l := len(bp.Points())

		if l > 0 {
			client, err := app.conf.getInfluxClient(topic)
			host := app.conf.getTopicConf(topic).Host
			if err != nil {
				return errwrap.Wrapf("Failed to create the InfluxDB client: {{err}}", err)
			}
			defer client.Close()
			err = client.Write(bp)
			if err != nil {
				log.WithError(err).
					WithField("host", host).
					WithField("database", dbname).
					WithField("topic", topic).
					Error("Error happened when writing points to InfluxDB")
				return errwrap.Wrapf("Writing points to InfluxDB failed: {{err}}", err)
			} else {
				log.WithField("nb_points", l).
					WithField("host", host).
					WithField("database", dbname).
					WithField("topic", topic).
					Info("Points written to InfluxDB")
			}
		}
	}
	return nil
}

func (app *Kafka2InfluxdbApp) consume() (total_count uint64, err error, reload bool, stopping bool) {

	total_count = 0
	reload = false
	stopping = false
	var last_push time.Time
	start_time := time.Now()

	topics, err := app.getSourceKafkaTopics()

	if err != nil {
		return 0, err, false, false
	}

	if len(topics) == 0 {
		err = fmt.Errorf("No kafka topic is matching: doing nothing")
		return 0, err, false, false
	}

	app.conf.cacheTopicsConfs(topics)

	for _, topic := range topics {
		topic_conf := app.conf.getTopicConf(topic)

		if topic_conf.Auth {
			err = app.createWriteUser(topic)
			if err != nil {
				return 0, err, false, false
			}
		}

		if topic_conf.CreateDatabases {
			err = app.createDatabase(topic)
			if err != nil {
				return 0, err, false, false
			}
		}

		if topic_conf.Auth {
			app.grantDatabaseRights(topic)
		}

		err = app.checkDatabase(topic)
		if err != nil {
			return 0, err, false, false
		}
	}

	sarama_conf, _ := app.conf.getSaramaClusterConf()
	consumer, err := cluster.NewConsumer(app.conf.Kafka.Brokers, app.conf.Kafka.ConsumerGroup, topics, sarama_conf)
	if err != nil {
		err = errwrap.Wrapf("Failed to create the Kafka consumer: {{err}}", err)
		return 0, err, false, false
	}
	defer consumer.Close()

	log.WithField("topics", strings.Join(topics, ",")).Info("Consuming these topics")

	stopping_signals := make(chan os.Signal, 1)
	signal.Notify(stopping_signals, syscall.SIGTERM, syscall.SIGINT)

	reload_signals := make(chan os.Signal, 1)
	signal.Notify(reload_signals, syscall.SIGHUP)

	pack_of_messages := make([]Message, 0, app.conf.BatchSize)
	last_push = time.Now()
	batch_max_duration := time.Millisecond * time.Duration(app.conf.BatchMaxDuration)
	refresh_topics_duration := time.Millisecond * time.Duration(app.conf.RefreshTopics)

	raw_messages := make(chan *sarama.ConsumerMessage, app.conf.BatchSize)
	parsed_messages := make(chan Message, app.conf.BatchSize)
	buckets := make(chan []Message, 100)
	push_errors := make(chan error, 100)

	var parser_workers_wg sync.WaitGroup
	var push_worker_wg sync.WaitGroup

	parse_worker := func() {
		for msg := range raw_messages {
			topic := msg.Topic
			topic_conf := app.conf.getTopicConf(topic)
			parser := NewParser(topic_conf.Format, topic_conf.Precision)
			point, err := parser.Parse(msg.Value)
			if err != nil {
				log.WithError(err).
					WithField("message", string(msg.Value)).
					Error("Error happened when parsing a metric")
			} else {
				parsed_messages <- Message{raw: msg, parsed: point}
			}
		}
		parser_workers_wg.Done()
	}

	push_worker := func() {
		for bucket := range buckets {
			err := app.process(bucket)
			if err != nil {
				push_errors <- err
				continue
			}
			stash := cluster.NewOffsetStash()
			for _, m := range bucket {
				stash.MarkOffset(m.raw, "")
			}
			consumer.MarkOffsets(stash)
			err = consumer.CommitOffsets()
			if err != nil {
				push_errors <- errwrap.Wrapf("Error happened while committing offsets to kafka: {{err}}", err)
			}
		}
		close(push_errors)
		push_worker_wg.Done()
	}

	// defers are triggered in LIFO
	defer func() {
		push_worker_wg.Wait()
		parser_workers_wg.Wait()
		close(parsed_messages)
	}()
	defer close(raw_messages)
	defer close(buckets)

	num_cpus := runtime.NumCPU()
	for i := 0; i < num_cpus; i++ {
		parser_workers_wg.Add(1)
		go parse_worker()
	}
	push_worker_wg.Add(1)
	go push_worker()

	for {
		select {
		case <-stopping_signals:
			log.Info("Caught SIGTERM signal: stopping")
			return total_count, nil, false, true
		case <-reload_signals:
			log.Info("Caught SIGHUP signal: reloading configuration")
			return total_count, nil, true, false
		case err, more := <-push_errors:
			if more {
				return total_count, err, false, false
			}
		case err, more := <-consumer.Errors():
			if more {
				err = errwrap.Wrapf("Error happened in kafka consumer: {{err}}", err)
				return total_count, err, false, false
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				log.WithField("ntf", fmt.Sprintf("%+v", ntf)).Debug("Rebalanced")
			}

		default:
			select {
			case msg, more := <-parsed_messages:
				if more {
					pack_of_messages = append(pack_of_messages, msg)
					now := time.Now()
					if (len(pack_of_messages) >= int(app.conf.BatchSize)) || (now.Sub(last_push) > batch_max_duration) {
						last_push = now
						buckets <- pack_of_messages
						pack_of_messages = make([]Message, 0, app.conf.BatchSize)
						if time.Now().Sub(start_time) > refresh_topics_duration {
							// time to refresh topics
							log.Info("Refreshing topics")
							return total_count, nil, false, false
						}
					}
				}
			default:
				select {
				case msg, more := <-consumer.Messages():
					if more {
						total_count++
						raw_messages <- msg
					}
				default:
				}
			}
		}
	}
}
