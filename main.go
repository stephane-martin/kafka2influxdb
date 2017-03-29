package main

import (
	"fmt"
	"log/syslog"
	"os"
	"os/exec"
	"syscall"
	"github.com/Sirupsen/logrus"
	logrus_syslog "github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/alecthomas/kingpin"
	"github.com/facebookgo/pidfile"
)

var log *logrus.Logger

func init() {
	log = logrus.New()
}

var (
	kapp = kingpin.New("kafka2influxdb", "Get metrics from Kafka and push them to InfluxDB")

	config_fname = kapp.Flag("config", "configuration directory").Default("/etc/kafka2influxdb").String()

	check_topics_cmd = kapp.Command("check-topics", "Print which topics in Kafka will be pulled")
	default_conf_cmd = kapp.Command("default-config", "print default configuration")
	check_conf_cmd   = kapp.Command("check-config", "check configuration")
	ping_influx_cmd  = kapp.Command("ping-influxdb", "check connection to influxdb")

	start_cmd          = kapp.Command("start", "start kafka2influxdb")
	daemonize_flag     = start_cmd.Flag("daemonize", "start kafka2influxdb as a daemon").Default("false").Bool()
	syslog_flag        = start_cmd.Flag("syslog", "send logs to local syslog").Default("false").Bool()
	logfile_flag       = start_cmd.Flag("logfile", "write logs to some file instead of stdout/stderr").Default("").String()
	loglevel_flag      = start_cmd.Flag("loglevel", "logging level").Default("info").String()
	start_pidfile_flag = start_cmd.Flag("pidfile", "if specified, write PID file there").Default("").String()

	stop_cmd          = kapp.Command("stop", "stop influx2influxdb")
	stop_pidfile_flag = stop_cmd.Flag("pidfile", "use this pidfile to find the process to kill").Default("").String()

	install_cmd = kapp.Command("install", "install kafka2influxdb")
	prefix_flag = install_cmd.Flag("prefix", "installation prefix").Default("/usr/local").String()
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
			output, err := exec.Command("pkill", "-SIGTERM", "kafka2influxdb").CombinedOutput()
			if err != nil {
				log.WithError(err).WithField("output", output).Fatal("failed to (p)kill kafka2influxdb")
			}
		}

	case start_cmd.FullCommand():
		// read the configuration file
		config_ptr, err := ReadConfig(*config_fname)
		if err != nil {
			log.WithField("error", err).Fatal("Failed to read configuration file")
		}
		// check that configuration is OK
		err = config_ptr.check()
		if err != nil {
			log.WithError(err).Fatal("Incorrect configuration")
		}
		// check we can write to the the desired logfile
		if len(*logfile_flag) > 0 {
			logfile, err := os.OpenFile(*logfile_flag, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)
			if err != nil {
				log.WithError(err).WithField("logfile", *logfile_flag).Fatal("Failed to open the logfile")
			}
			logfile.Close()
		}
		
		app := Kafka2InfluxdbApp{conf: config_ptr}

		// check we can connect to InfluxDB
		_, _, _, err = app.pingInfluxDB()
		if err != nil {
			log.WithError(err).Fatal("Ping InfluxDB failed")
		}

		if (*daemonize_flag) {
			// daemon dance
			
			// lock stdout to avoid a race condition
			err := syscall.FcntlFlock(os.Stdout.Fd(), syscall.F_SETLKW, &syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: 0, Len: 0 })
			if err != nil { log.WithError(err).Fatal("Failed to lock stdout") }
			if os.Getppid() != 1 {
				// parent
				selfpath, err := exec.LookPath(os.Args[0])
				if err != nil { log.WithError(err).Fatal("Failed to lookup binary") }
				_, err = os.StartProcess(selfpath, os.Args, &os.ProcAttr{Dir: "", Env: nil, Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}, Sys: nil})
				if err != nil { log.WithError(err).Fatal("Failed to start process") }
				os.Exit(0)
			} else {
				// child
				_, err := syscall.Setsid()
				if err != nil { log.WithError(err).Fatal("Failed to create new session") }
				devnullfile, err := os.OpenFile("/dev/null", os.O_RDWR, 0)
				if err != nil { log.WithError(err).Fatal("Failed to open /dev/null") }
				syscall.Dup2(int(devnullfile.Fd()), int(os.Stdin.Fd()))
				syscall.Dup2(int(devnullfile.Fd()), int(os.Stdout.Fd()))
				syscall.Dup2(int(devnullfile.Fd()), int(os.Stderr.Fd()))
				devnullfile.Close()
			}
		}
		do_start_real(&app, *start_pidfile_flag, *syslog_flag, *loglevel_flag, *logfile_flag)

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

	case install_cmd.FullCommand():
		// todo: copy executable, man page and init service
	}

}

func do_start_real(app *Kafka2InfluxdbApp, pidfilename string, use_syslog bool, loglevel string, logfilename string) {
	var total_count uint64 = 0
	var count uint64 = 0
	var err error
	restart := true

	if use_syslog {
		// also write logs to syslog
		hook, err := logrus_syslog.NewSyslogHook("", "", syslog.LOG_INFO, "")
		if err == nil {
			log.Formatter = &logrus.TextFormatter{DisableColors: true, DisableTimestamp: true}
			log.Hooks.Add(hook)
		} else {
			log.WithError(err).Error("Unable to connect to local syslog daemon")
		}
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

	// start the consuming loop
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
}
