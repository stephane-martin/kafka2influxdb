package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
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

func (app *Kafka2InfluxdbApp) pingInfluxDB() (version string, dbnames []string, users []string, err error) {
	dbnames = []string{}
	users = []string{}

	influx_config, err := app.conf.getInfluxHTTPConfig(true)
	if err != nil {
		log.WithError(err).Error("Can't build InfluxDB configuration")
		return
	}
	client, err := influx.NewHTTPClient(influx_config)
	if err != nil {
		log.WithError(err).Error("Error connecting to InfluxDB")
		return
	}
	defer client.Close()
	d, version, err := client.Ping(time.Duration(app.conf.Influxdb.Timeout) * time.Millisecond)
	if err != nil {
		log.WithError(err).WithField("duration", d).Error("Ping failed")
		return
	}
	dbnames, err = listExistingDatabases(client)
	if err != nil {
		log.WithError(err).Error("Error listing existing databases in InfluxDB")
		return
	}
	users, err = listExistingUsers(client)
	if err != nil {
		log.WithError(err).Error("Error listing users in InfluxDB")
		return
	}
	return
}

func (app *Kafka2InfluxdbApp) createWriteUser() error {

	if app.conf.Influxdb.Username == app.conf.Influxdb.AdminUsername {
		// only one kind of influxdb user was provided
		// if that user is admin, nothing to do (already exists, right ?)
		// if that user is not admin, we don't have an admin account to create it
		// so let's give up
		return nil
	}

	// connecting to influxDB (as admin)
	config, err := app.conf.getInfluxHTTPConfig(true)
	if err != nil {
		log.WithError(err).
			WithField("function", "createDatabases").
			Error("Error building configuration for InfluxDB")
		return err
	}

	client, err := influx.NewHTTPClient(config)
	if err != nil {
		log.WithError(err).
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
			WithField("function", "createWriteUser").
			Error("Error listing users in Influxdb")
		return err
	}

	already_exists := func() bool {
		for _, existing_user := range existing_users {
			if existing_user == app.conf.Influxdb.Username {
				return true
			}
		}
		return false
	}

	if already_exists() {
		log.WithField("username", app.conf.Influxdb.Username).
			Info("User already exists in InfluxDB")
	} else {
		// creating new user
		q := influx.NewQuery(
			fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD '%s'`,
				strings.Replace(app.conf.Influxdb.Username, `"`, `\"`, -1),
				strings.Replace(app.conf.Influxdb.Password, `'`, `\'`, -1)),
			"", "")
		resp, err := client.Query(q)
		if err != nil {
			log.WithError(err).
				WithField("username", app.conf.Influxdb.Username).
				WithField("function", "createWriteUser").
				Error("Error creating user in InfluxDB")
			return errwrap.Wrapf("Failed to create user in InfluxDB: {{err}}", err)
		} else if resp.Error() != nil {
			log.WithError(resp.Error()).
				WithField("username", app.conf.Influxdb.Username).
				WithField("function", "createWriteUser").
				Error("Error creating user in InfluxDB")
			return errwrap.Wrapf("Failed to create user in InfluxDB: {{err}}", resp.Error())
		} else {
			log.WithField("username", app.conf.Influxdb.Username).
				Info("Created user in InfluxDB")
		}
	}
	return nil
}

func (app *Kafka2InfluxdbApp) createDatabases(topics []string) error {
	// connecting to influxDB (as a admin user)
	config, err := app.conf.getInfluxHTTPConfig(true)
	if err != nil {
		log.WithError(err).
			WithField("function", "createDatabases").
			Error("Error building configuration for InfluxDB")
		return err
	}

	client, err := influx.NewHTTPClient(config)
	if err != nil {
		log.WithError(err).
			WithField("function", "createDatabases").
			Error("Error connecting to InfluxDB")
		return err
	}
	defer client.Close()

	// list the destination databases in InfluxDB
	must_exist := app.conf.listTargetDatabases(topics)

	// list exiting databases
	existing_databases, err := listExistingDatabases(client)
	if err != nil {
		log.WithError(err).
			WithField("function", "createDatabases").
			Error("Error listing existing databases")
		return err
	}

	existing_databases_map := map[string]bool{}
	for _, dbname := range existing_databases {
		existing_databases_map[dbname] = true
	}

	// list the databases that have to be created
	to_be_created := []string{}
	for _, dbname := range must_exist {
		if !existing_databases_map[dbname] {
			to_be_created = append(to_be_created, dbname)
		}
	}

	createDatabase := func(dbname string) error {
		q := influx.NewQuery(fmt.Sprintf("CREATE DATABASE %s", dbname), "", "")
		resp, err := client.Query(q)
		if err != nil {
			return errwrap.Wrapf("CREATE DATABASE failed: {{err}}", err)
		} else if resp.Error() != nil {
			return errwrap.Wrapf("CREATE DATABASE failed: {{err}}", resp.Error())
		}
		return nil
	}

	// create the missing databases
	for _, dbname := range to_be_created {
		if err := createDatabase(dbname); err != nil {
			log.WithError(err).
				WithField("action", "creating influxdb database").
				WithField("function", "createDatabases").
				WithField("dbname", dbname).
				Error("Error creating a database in InfluxDB")
			return err
		} else {
			log.WithField("database", dbname).Info("Created database")
		}
	}
	return nil
}

func (app *Kafka2InfluxdbApp) grantDatabaseRights(topics []string) {
	// try to grant read and write rights on the target databases to the
	// "normal" influxdb user

	// connecting to influxDB (as a normal user)
	config, err := app.conf.getInfluxHTTPConfig(false)
	if err != nil {
		log.WithError(err).
			WithField("function", "grantDatabaseRights").
			Error("Error building configuration for InfluxDB")
		return
	}

	client, err := influx.NewHTTPClient(config)
	if err != nil {
		log.WithError(err).
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
		log.Info("InfluxDB normal user is in fact admin, no GRANT needs to be performed")
		return
	}

	// the normal user is not an admin
	log.Info("InfluxDB normal user is not an admin")

	// check that normal user and admin user are different
	if app.conf.Influxdb.Username == app.conf.Influxdb.AdminUsername {
		log.Info("No InfluxDB admin user was provided. Can't GRANT rights.")
		return
	}

	// connecting to influxDB (as admin user)
	admin_config, err := app.conf.getInfluxHTTPConfig(true)
	if err != nil {
		log.WithError(err).
			WithField("function", "grantDatabaseRights").
			Error("Error building configuration for InfluxDB")
		return
	}

	admin_client, err := influx.NewHTTPClient(admin_config)
	if err != nil {
		log.WithError(err).
			WithField("function", "grantDatabaseRights").
			Error("Error connecting to InfluxDB")
		return
	}
	defer admin_client.Close()

	// check that the provided InfluxDB user is truely admin
	q = influx.NewQuery("SHOW DATABASES", "", "")
	resp, err = admin_client.Query(q)
	if err != nil || resp.Error() != nil {
		log.Warn("The provided InfluxDB admin user does not really have admin rights.")
		return
	}

	// list target databases
	target_databases := app.conf.listTargetDatabases(topics)

	for _, dbname := range target_databases {
		// use admin user to grant rights on the InfluxDB target databases
		// (the GRANT will succeed even if the target database does not exist)
		// todo: escaping
		q_str := fmt.Sprintf("GRANT ALL ON %s to %s", dbname, app.conf.Influxdb.Username)
		q := influx.NewQuery(q_str, dbname, "")
		resp, err = admin_client.Query(q)
		if err != nil {
			log.WithError(err).
				WithField("username", app.conf.Influxdb.Username).
				WithField("database", dbname).
				Info("Failed to GRANT rights")
		} else if resp.Error() != nil {
			log.WithError(resp.Error()).
				WithField("username", app.conf.Influxdb.Username).
				WithField("database", dbname).
				Info("Failed to GRANT rights")
		} else {
			log.WithField("database", dbname).
				WithField("username", app.conf.Influxdb.Username).
				Info("GRANTed rights")
		}
	}
}

func (app *Kafka2InfluxdbApp) checkDatabases(topics []string) (err error) {
	// check that all required databases exist and that the influxdb user can connect to them

	// connecting to influxDB (as a normal user)
	config, err := app.conf.getInfluxHTTPConfig(false)
	if err != nil {
		log.WithError(err).
			WithField("function", "checkDatabases").
			Error("Error building configuration for InfluxDB")
		return err
	}

	client, err := influx.NewHTTPClient(config)
	if err != nil {
		log.WithError(err).
			WithField("function", "checkDatabases").
			Error("Error connecting to InfluxDB")
		return err
	}
	defer client.Close()

	// list the destination databases in InfluxDB
	must_exist := app.conf.listTargetDatabases(topics)

	// try to connect to each DB
	dbnames_error := []string{}
	for _, dbname := range must_exist {
		q := influx.NewQuery("SHOW SERIES", dbname, "")
		resp, err := client.Query(q)
		if err != nil || resp.Error() != nil {
			dbnames_error = append(dbnames_error, dbname)
			e := err
			if err == nil {
				e = resp.Error()
			}
			log.WithError(e).
				WithField("database", dbname).
				WithField("function", "checkDatabases").
				Error("Failed to connect")
		}
	}

	if len(dbnames_error) == 0 {
		// we can connect to each DB, OK
		return nil
	}
	for _, dbname := range dbnames_error {
		log.WithField("database", dbname).Error("Not enough rights on an InfluxDB database")
	}
	return fmt.Errorf("Error connecting to some databases")
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

func (app *Kafka2InfluxdbApp) process(pack []*sarama.ConsumerMessage) (err error) {
	config, err := app.conf.getInfluxHTTPConfig(false)
	if err != nil {
		return err
	}

	client, err := influx.NewHTTPClient(config)
	if err != nil {
		return errwrap.Wrapf("Failed to create the InfluxDB client: {{err}}", err)
	}
	defer client.Close()

	log.WithField("nb_points", len(pack)).Info("Points to push to InfluxDB")
	topicBatchMap := map[string]influx.BatchPoints{}


	parser, err := NewParser(app.conf.Kafka.Format, app.conf.Influxdb.Precision)
	if err != nil {
		return err
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
		point, err := parser.Parse(msg.Value)
		if err == nil {
			if point != nil {
				topicBatchMap[msg.Topic].AddPoint(point)
			}
		} else {
			log.WithError(err).
				WithField("message", msg.Value).
				Error("error happened when parsing a metric")
		}
	}
	for topic, bp := range topicBatchMap {
		dbname := bp.Database()
		l := len(bp.Points())

		if l > 0 {
			err := client.Write(bp)
			if err != nil {
				log.WithError(err).
					WithField("database", dbname).
					WithField("topic", topic).
					Error("Error happened when writing points to InfluxDB")
				return errwrap.Wrapf("Writing points to InfluxDB failed: {{err}}", err)
			} else {
				log.WithField("nb_points", l).
					WithField("database", dbname).
					WithField("topic", topic).
					Info("Points written to InfluxDB")
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
	err = consumer.CommitOffsets()
	if err != nil {
		return errwrap.Wrapf("Error happened while committing offsets to kafka: {{err}}", err)
	}

	return nil
}

func (app *Kafka2InfluxdbApp) consume() (total_count uint64, err error, restart bool) {

	total_count = 0
	restart = false

	var count uint32 = 0
	var last_push time.Time
	start_time := time.Now()

	topics, err := app.getSourceKafkaTopics()

	if err != nil {
		return
	}

	if len(topics) == 0 {
		log.Warn("No Kafka topic is matching your glob")
		return
	}

	if app.conf.Influxdb.Auth {
		err = app.createWriteUser()
		if err != nil {
			return
		}
	}

	if app.conf.Influxdb.CreateDatabases {
		err = app.createDatabases(topics)
		if err != nil {
			return
		}
	}

	if app.conf.Influxdb.Auth {
		app.grantDatabaseRights(topics)
	}

	err = app.checkDatabases(topics)
	if err != nil {
		return
	}

	sarama_conf, _ := app.conf.getSaramaClusterConf()
	consumer, err := cluster.NewConsumer(app.conf.Kafka.Brokers, app.conf.Kafka.ConsumerGroup, topics, sarama_conf)
	if err != nil {
		log.WithError(err).Error("Error creating the Kafka consumer")
		err = errwrap.Wrapf("Failed to create the Kafka consumer: {{err}}", err)
		return 0, err, false
	}
	defer consumer.Close()

	log.WithField("topics", strings.Join(topics, ",")).Info("Will consume from these fields")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	pack_of_messages := []*sarama.ConsumerMessage{}
	last_push = time.Now()
	batch_max_duration := time.Millisecond * time.Duration(app.conf.BatchMaxDuration)
	refresh_topics_duration := time.Millisecond * time.Duration(app.conf.RefreshTopics)

	for {
		select {
		case <-signals:
			log.Info("Caught signal")
			return total_count, nil, false
		default:
			select {
			case msg, more := <-consumer.Messages():
				if more {
					count++
					total_count++
					pack_of_messages = append(pack_of_messages, msg)
					now := time.Now()
					if (count >= app.conf.BatchSize) || (now.Sub(last_push) > batch_max_duration) {
						count = 0
						last_push = now
						err := app.processAndCommit(consumer, &pack_of_messages)
						if err != nil {
							return total_count, err, false
						}
						pack_of_messages = []*sarama.ConsumerMessage{}
						if time.Now().Sub(start_time) > refresh_topics_duration {
							// time to refresh topics
							log.Info("Refreshing topics")
							return total_count, nil, true
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
			}
		}
	}
}
