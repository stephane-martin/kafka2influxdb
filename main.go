package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log/syslog"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	logrus_syslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/alecthomas/kingpin"
	"github.com/facebookgo/pidfile"
	"github.com/shirou/gopsutil/process"
	findinit "github.com/stephane-martin/go-findinit"
)

var log *logrus.Logger

func init() {
	log = logrus.New()
}

func SighupMyself() {
	p, _ := os.FindProcess(os.Getpid())
	err := p.Signal(syscall.SIGHUP)
	if err != nil {
		log.WithError(err).Error("Error sending SIGHUP to self")
	}
}

var (
	kapp = kingpin.New("kafka2influxdb", "Get metrics from Kafka and push them to InfluxDB")

	config_fname      = kapp.Flag("config", "configuration directory").Default("/etc/kafka2influxdb").String()
	consul_addr       = kapp.Flag("consul-addr", "consul address (http://ADDR:PORT)").Default("").String()
	consul_prefix     = kapp.Flag("consul-prefix", "consul KV prefix").Default("kafka2influxdb").String()
	consul_token      = kapp.Flag("consul-token", "token to connect to consul").Default("").String()
	consul_datacenter = kapp.Flag("consul-datacenter", "from which consul datacenter to load configuration").Default("").String()

	check_topics_cmd = kapp.Command("check-topics", "Print which topics in Kafka will be pulled")
	default_conf_cmd = kapp.Command("default-config", "print default configuration")
	check_conf_cmd   = kapp.Command("check-config", "check configuration")
	ping_influx_cmd  = kapp.Command("ping-influxdb", "check connection to influxdb")

	start_cmd          = kapp.Command("start", "start kafka2influxdb")
	daemonize_flag     = start_cmd.Flag("daemonize", "start kafka2influxdb as a daemon").Default("false").Bool()
	syslog_flag        = start_cmd.Flag("syslog", "send logs to local syslog").Default("false").Bool()
	logfile_flag       = start_cmd.Flag("logfile", "write logs to some file instead of stdout/stderr").Default("").String()
	loglevel_flag      = start_cmd.Flag("loglevel", "logging level").Default("info").String()
	logformat_flag     = start_cmd.Flag("logformat", "logging format ('text' or 'json')").Default("text").String()
	start_pidfile_flag = start_cmd.Flag("pidfile", "if specified, write PID file there").Default("").String()
	watch_config_flag  = start_cmd.Flag("watchconf", "if specified, kafka2influxdb will reload itself on configuration file change").Default("false").Bool()

	stop_cmd          = kapp.Command("stop", "stop influx2influxdb")
	stop_pidfile_flag = stop_cmd.Flag("pidfile", "use this pidfile to find the process to kill").Default("").String()

	install_cmd = kapp.Command("install", "install kafka2influxdb")
	prefix_flag = install_cmd.Flag("prefix", "installation prefix").Default("/usr/local").String()
)

func main() {
	cmd := kingpin.MustParse(kapp.Parse(os.Args[1:]))

	switch cmd {

	case ping_influx_cmd.FullCommand():
		app := Kafka2InfluxdbApp{}
		_, err := app.reloadConfiguration(*config_fname, *consul_addr, *consul_prefix, *consul_token, *consul_datacenter, nil)
		if err != nil {
			log.WithError(err).Fatal("Failed to load configuration")
		}
		versions, err := app.pingInfluxDB()
		if err != nil {
			log.WithError(err).Fatal("Ping InfluxDB failed")
		}
		for mapping_name, version := range versions {
			fmt.Printf("%s => InfluxDB version %s\n", mapping_name, version)
		}

	case stop_cmd.FullCommand():
		if len(*stop_pidfile_flag) > 0 {
			pidfile.SetPidfilePath(*stop_pidfile_flag)
			pid, err := pidfile.Read()
			if err != nil {
				log.WithError(err).Fatal("failed to read PID file")
			}
			process, err := os.FindProcess(pid)
			if err != nil {
				log.WithError(err).WithField("pid", pid).Fatal("failed to find process with such PID")
			}
			err = process.Signal(syscall.SIGTERM)
			if err != nil {
				log.WithError(err).WithField("pid", pid).Fatal("failed to send SIGTERM to process")
			}
		} else {
			my_pid := os.Getpid()
			pid_list, err := process.Pids()
			if err != nil {
				log.WithError(err).Fatal("Failed to get the list of processes")
			}
			for _, pid := range pid_list {
				p, err := process.NewProcess(pid)
				if err == nil {
					name, err := p.Name()
					if err == nil && int(pid) != my_pid && strings.Contains(name, "kafka2influxdb") {
						p.Terminate()
					}
				}
			}
		}

	case start_cmd.FullCommand():
		app := NewApp()
		_, err := app.reloadConfiguration(*config_fname, *consul_addr, *consul_prefix, *consul_token, *consul_datacenter, nil)
		if err != nil {
			log.WithError(err).Fatal("Failed to load the configuration")
		}
		// check we can write to the the desired logfile
		if len(*logfile_flag) > 0 {
			logfile, err := os.OpenFile(*logfile_flag, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)
			if err != nil {
				log.WithError(err).WithField("logfile", *logfile_flag).Fatal("Failed to open the logfile")
			}
			logfile.Close()
		}

		if *daemonize_flag {
			// daemon dance

			// lock stdout to avoid a race condition
			err := syscall.FcntlFlock(os.Stdout.Fd(), syscall.F_SETLKW, &syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: 0, Len: 0})
			if err != nil {
				log.WithError(err).Fatal("Failed to lock stdout")
			}
			if os.Getppid() != 1 {
				// parent
				selfpath, err := exec.LookPath(os.Args[0])
				if err != nil {
					log.WithError(err).Fatal("Failed to lookup binary")
				}
				_, err = os.StartProcess(selfpath, os.Args, &os.ProcAttr{Dir: "", Env: nil, Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}, Sys: nil})
				if err != nil {
					log.WithError(err).Fatal("Failed to start process")
				}
				os.Exit(0)
			} else {
				// child
				_, err := syscall.Setsid()
				if err != nil {
					log.WithError(err).Fatal("Failed to create new session")
				}
				devnullfile, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
				if err != nil {
					log.WithError(err).Fatal("Failed to open /dev/null")
				}
				syscall.Dup2(int(devnullfile.Fd()), int(os.Stdin.Fd()))
				syscall.Dup2(int(devnullfile.Fd()), int(os.Stdout.Fd()))
				syscall.Dup2(int(devnullfile.Fd()), int(os.Stderr.Fd()))
				devnullfile.Close()
			}
		}
		app.InfluxMetrics()
		do_start_real(app, *config_fname, *start_pidfile_flag, *syslog_flag, *loglevel_flag, *logfile_flag)

	case check_topics_cmd.FullCommand():
		app := NewApp()
		_, err := app.reloadConfiguration(*config_fname, *consul_addr, *consul_prefix, *consul_token, *consul_datacenter, nil)
		if err != nil {
			log.WithError(err).Fatal("Failed to load configuration")
		}
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
		fmt.Println(defaultConfiguration().export())

	case check_conf_cmd.FullCommand():
		app := NewApp()
		_, err := app.reloadConfiguration(*config_fname, *consul_addr, *consul_prefix, *consul_token, *consul_datacenter, nil)

		if err != nil {
			log.WithError(err).Fatal("Failed to load configuration")
		}

		fmt.Println("Configuration looks OK\n")
		fmt.Println(app.conf.export())

	case install_cmd.FullCommand():
		if !filepath.IsAbs(*prefix_flag) {
			log.WithField("prefix", *prefix_flag).Fatal("prefix is not absolute")
		}

		user_id := "_kafka2influxdb"
		group_id := "_kafka2influxdb"
		log_path := "/var/log/kafka2influxdb"

		g, err := user.LookupGroup(group_id)
		if _, ok := err.(user.UnknownGroupError); ok {
			o, err := exec.Command("groupadd", "--system", group_id).CombinedOutput()
			if err == nil {
				log.WithField("group_id", group_id).Info("Created group")
			} else {
				log.WithField("output", string(o)).WithField("group_id", group_id).WithError(err).Fatal("Failed to create group")
			}
			g, _ = user.LookupGroup(group_id)
		} else {
			log.WithField("group_id", group_id).Info("group already exists")
		}
		gid, _ := strconv.ParseInt(g.Gid, 10, 16)

		u, err := user.Lookup(user_id)
		if _, ok := err.(user.UnknownUserError); ok {
			o, err := exec.Command("useradd", "-d", "/nonexistent", "-g", group_id, "-M", "-N", "--system", "-s", "/bin/false", user_id).CombinedOutput()
			if err == nil {
				log.WithField("user_id", user_id).Info("Created user")
			} else {
				log.WithField("output", string(o)).WithField("user_id", user_id).WithError(err).Fatal("Failed to create user")
			}
			u, _ = user.Lookup(user_id)
		} else {
			log.WithField("user_id", user_id).Info("user already exists")
		}
		uid, _ := strconv.ParseInt(u.Uid, 10, 16)

		if _, err := os.Stat(log_path); os.IsNotExist(err) {
			err = os.MkdirAll(log_path, 0750)
			if err != nil {
				log.WithError(err).WithField("logpath", log_path).Fatal("Failed to create the log directory")
			} else {
				log.WithField("logpath", log_path).Info("Created log directory")
			}
		} else {
			log.WithField("logpath", log_path).Info("Log directory already exists")
		}

		os.Chown(log_path, int(uid), int(gid))

		selfpath, err := exec.LookPath(os.Args[0])
		if err != nil {
			log.WithError(err).Fatal("Failed to lookup binary")
		}
		selfpath, err = filepath.Abs(selfpath)
		if err != nil {
			log.WithError(err).Fatal("Unexpected error ?!")
		}

		destpath, err := filepath.Abs(filepath.Join(*prefix_flag, "bin", "kafka2influxdb"))
		if err != nil {
			log.WithError(err).Fatal("Unexpected error ?!")
		}

		// copy the binary
		if selfpath != destpath {
			me, err := ioutil.ReadFile(selfpath)
			if err != nil {
				log.WithError(err).Fatal("Unexpected error ?!")
			}
			os.MkdirAll(filepath.Dir(destpath), 0755)
			err = ioutil.WriteFile(destpath, me, 0755)
			if err != nil {
				log.WithError(err).WithField("prefix", *prefix_flag).WithField("destination", destpath).Fatal("Error copying executable. Try sudo ?")
			} else {
				log.WithField("prefix", *prefix_flag).WithField("destination", destpath).Info("Wrote kafka2influxdb binary")
			}
		}

		// copy the manual page
		manpath := filepath.Join(*prefix_flag, "man", "man1", "kafka2influxdb.1")
		os.MkdirAll(filepath.Dir(manpath), 0755)
		mancontent, _ := docsKafka2influxdb1Bytes()
		err = ioutil.WriteFile(manpath, mancontent, 0644)
		if err != nil {
			log.WithError(err).WithField("prefix", *prefix_flag).WithField("manpath", manpath).Fatal("Failed to write the manual page")
		} else {
			log.WithField("prefix", *prefix_flag).WithField("manpath", manpath).Info("Wrote the manual page")
		}

		// create a default configuration file if it does not already exist
		confpath := filepath.Join(*prefix_flag, "etc", "kafka2influxdb", "kafka2influxdb.toml")
		if _, err := os.Stat(confpath); os.IsNotExist(err) {
			os.MkdirAll(filepath.Dir(confpath), 0755)
			err = ioutil.WriteFile(confpath, []byte(defaultConfiguration().export()), 0644)
			if err != nil {
				log.WithField("confpath", confpath).WithError(err).Fatal("Failed to write configuration file")
			} else {
				log.WithField("confpath", confpath).Info("Created a default configuration file")
			}
		} else {
			log.WithField("confpath", confpath).Info("Configuration file already exists")
		}

		// copy the right init service file
		init_kind := findinit.FindInit()
		var init_path string
		var init_content []byte

		switch init_kind {
		case findinit.Systemd:
			init_content, _ = kafka2influxdbServiceBytes()
			init_path = "/etc/systemd/system/kafka2influxdb.service"
		case findinit.Upstart:
			init_content, _ = kafka2influxdbUpstartBytes()
			init_path = "/etc/init/kafka2influxdb.conf"
		default:
			log.WithField("init", init_kind).Fatal("not implemented")
		}

		init_content = bytes.Replace(init_content, []byte("/usr/local"), []byte(*prefix_flag), -1)
		init_content = bytes.Replace(
			init_content,
			[]byte("/etc/kafka2influxdb"),
			[]byte(filepath.Dir(confpath)),
			-1)

		err = ioutil.WriteFile(init_path, init_content, 0644)
		if err != nil {
			log.WithError(err).WithField("path", init_path).Fatal("Failed to write the service file")
		} else {
			log.WithField("path", init_path).Info("Wrote the service file")
		}

		if init_kind == findinit.Upstart {
			// don't start the upstart service automatically
			if _, err = os.Stat("/etc/init/kafka2influxdb.override"); os.IsNotExist(err) {
				err = ioutil.WriteFile("/etc/init/kafka2influxdb.override", []byte("manual"), 0644)
				if err != nil {
					log.Fatal("Failed to write /etc/init/kafka2influxdb.override")
				}
			}
		}

		default_c := "/etc/default/kafka2influxdb"
		if _, err = os.Stat(default_c); os.IsNotExist(err) {
			os.MkdirAll(filepath.Dir(default_c), 0755)
			init_default_content, _ := kafka2influxdbDefaultBytes()
			err = ioutil.WriteFile(default_c, init_default_content, 0644)
			if err != nil {
				log.WithError(err).WithField("path", default_c).Fatal("Failed to write")
			} else {
				log.WithField("path", default_c).Info("Created")
			}
		} else {
			log.WithField("path", default_c).Info("Already exists")
		}

		// systemctl daemon-reload
		if init_kind == findinit.Systemd {
			exec.Command("systemctl", "daemon-reload").CombinedOutput()
		}
	}

}

func do_start_real(app *Kafka2InfluxdbApp, config_dirname string, pidfilename string, use_syslog bool, loglevel string, logfilename string) {
	var total_count uint64 = 0
	var err error
	var watcher *ConfigurationWatcher
	disable_timestamps := false

	signal.Ignore(syscall.SIGHUP)

	if use_syslog {
		// also write logs to syslog
		hook, err := logrus_syslog.NewSyslogHook("", "", syslog.LOG_INFO, "")
		if err == nil {
			disable_timestamps = true
			log.Hooks.Add(hook)
		} else {
			log.WithError(err).Error("Unable to connect to local syslog daemon")
		}
	}

	if *logformat_flag == "json" {
		log.Formatter = &logrus.JSONFormatter{}
	} else {
		log.Formatter = &logrus.TextFormatter{DisableColors: true, DisableTimestamp: disable_timestamps}
	}

	// set log level
	parsed_loglevel, err := logrus.ParseLevel(loglevel)
	if err != nil {
		parsed_loglevel = logrus.InfoLevel
	}
	log.Level = parsed_loglevel

	// write logs to a log file
	if len(logfilename) > 0 {
		logfile, err := os.OpenFile(logfilename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)
		if err != nil {
			log.WithError(err).WithField("filename", logfilename).Fatal("Failed to open the log file")
		}
		defer logfile.Close()
		log.Out = logfile
	}

	// write a PID file
	if len(pidfilename) > 0 {
		pidfile.SetPidfilePath(pidfilename)
		err = pidfile.Write()
		if err != nil {
			log.WithError(err).Fatal("Error writing PID file")
		}
		defer func() {
			os.Remove(pidfilename)
		}()
	}

	pause := app.conf.RetryDelay
	start_time := time.Now()

	// start the consuming loop
	for {
		notify_change_consul := make(chan bool)
		stop_watch_consul, err := app.reloadConfiguration(config_dirname, *consul_addr, *consul_prefix, *consul_token, *consul_datacenter, notify_change_consul)
		if err != nil {
			log.WithError(err).Error("Failed to reload configuration: aborting")
			break
		}
		if stop_watch_consul != nil {
			go func() {
			Loop:
				for {
					_, more := <-notify_change_consul
					if more {
						SighupMyself()
					} else {
						break Loop
					}
				}
			}()
		}
		if *watch_config_flag {
			watcher = NewConfigurationWatcher(app.conf.ConfigFilename)
			go func() { watcher.Watch() }()
		}
		count, err, stopping := app.consume()
		if *watch_config_flag {
			watcher.StopWatch()
		}
		sclose(stop_watch_consul)

		total_count += count
		if err == nil {
			if stopping {
				break
			}
			pause = app.conf.RetryDelay
		} else {
			log.WithError(err).Error("Error happened while consuming")
			if stopping || (app.conf.RetryDelay < 0) {
				break
			}
			if app.conf.RetryDelay > 0 {
				log.WithField("duration", pause).Info("Pausing")
				time.Sleep(time.Millisecond * time.Duration(pause))
				pause *= 2
			}
		}
	}

	log.Info("Shutting down")
	log.WithField("total_count", total_count).Info("Total number of points fetched from Kafka")
	rate := int(float64(total_count) / time.Now().Sub(start_time).Seconds())
	log.WithField("rate", rate).Info("Messages per second")
}
