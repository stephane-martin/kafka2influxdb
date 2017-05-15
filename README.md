# Motivation

[InfluxDB](https://github.com/influxdata/influxdb) is a time-series database.
You can typically store metrics about your servers and applications in InfluxDB.

[Telegraf](https://github.com/influxdata/telegraf) can be used to collect
various metrics. With Telegraf, you could directly write collected metrics to
InfluxDB.

However, an indirection may be useful between Telegraf and InfluxDB.

-   Maybe you don't wish to expose directly InfluxDB to all your servers and
    apps for security or network segmentation
-   Your InfluxDB database may not be always available
-   High metrics trafic may require some buffering
-   You'd like to perform real-time stream processing on the metrics before
    they reach InfluxDB

In that case, Telegraf [can push the metrics to a Kafka cluster](https://github.com/influxdata/telegraf/tree/master/plugins/outputs/kafka).

kafka2finfluxdb provides a simple way to pull the metrics from Kafka and push
them to InfluxDB.

# Important notes

-   15/05/2017: configuration breaking change. InfluxDB TLS parameters have
    been merged into the main InfluxDB sections (`topic_conf`). Kafka TLS and SASL
    parameters have been merged into the main Kafka section. The change made
    parsing configuration from Consul easier.
-   15/05/2017: kafka2influxdb can now read its configuration from file and
    from Consul. The Consul configuration has precedence.

# Installation

Just `go get github.com/stephane-martin/kafka2influxdb`

The dependencies are vendored.

After compilation, you can `sudo ./kafka2influxdb install --prefix=/usr/local`
to copy the binary in the PATH and create a default configuration, the
systemd/upstart service, the log directory, a specific user/group, etc.

# Debian packaging

You can build a package for Debian based distributions.

First install `go`, then:

```
git clone https://github.com/stephane-martin/kafka2influxdb.git"
cd kafka2influxdb && make deb && cd ..
```

(If needed, it will `sudo` to install `devscripts` and `dh-systemd`.)

-   The package installs a systemd service (`kafka2influxdb`) but does not enable
it and does not start the service at package installation.
-   The configuration file is `/etc/kafka2influxdb/kafka2influxdb.toml`.
-   Logs are put in `/var/log/kafka2influxdb/kafka2influxdb.log`.

# File-based configuration

See [the configuration example](https://github.com/stephane-martin/kafka2influxdb/blob/master/kafka2influxdb.example.toml).

The configuration directory can be specified by a commandline flag
`--config=XXX` (it defaults to `/etc/kafka2influxdb`).

In that directory, the configuration filename must be `kafka2influxdb.toml`.

# Retrieve configuration from Consul KV

You can use the key-value store of Consul to store kafka2influxdb configuration.
To enable the feature, specify a `--consul-addr=http://ADDR:PORT` flag.

Command examples:

```
kafka2influxdb --consul-addr=http://10.75.1.1:8500 check-config
kafka2influxdb --consul-addr=http://10.75.1.1:8500 start
```

How to store the parameters in Consul KV:

```
kafka2influxdb/kafka/client_id              => my_client_id
kafka2influxdb/kafka/consumer_group         => kafka2influx-cg
kafka2influxdb/kafka/version                => 0.10.1.1
kafka2influxdb/topic_conf/default/dbname    => generic
kafka2influxdb/topic_conf/default/precision => s
kafka2influxdb/topic_conf/mail/dbname       => mailmetrics
```

TOML array parameters must be translated into comma-separated values in Consul
(eg. for the list of topics to consume from, or for the list of Kafka brokers).

```
kafka2influxdb/kafka/brokers => kafka1.vpn:9092,kafka2.vpn:9092,kafka3.vpn:9092 (comma-separated list)
```

Also note that the global TOML parameters that don't belong to any section are
to be put in the `kafka2influxdb/global` prefix. Eg:

```
kafka2influxdb/global/refresh_topics => 200000
kafka2influxdb/global/topics         => telegraf_*,mymetrics                    (comma-separated list)
```

The ordered list of 'mappings' (`[[mapping]]` sections in TOML) must be stored
at the Consul key `kafka2influxdb/mappings`. Just put your mappings there, one
mapping per line. For example the value for the `kafka2inflxudb/mappings` key
could be something like:

```
"*pgsql*"                            = "database"
"*mysql*"                            = "database"
"*ldap*"                             = "database"
"*webmail*"                          = "web"
"*vmail*"                            = "mail"
"*"                                  = "default"
```

(Each line is parsed as TOML).

The command `check-config` can be used to check the resulting configuration.

When Consul configuration is used with the `start` command, `kafka2influxdb`
monitors the Consul KV store for configuration changes, and restarts itself if
necessary.

# Running

See `kafka2influxdb --help`.

kafka2influxdb uses a Kafka consumer group. Throughput may be increased by
running multiple instances of kafka2influxdb. In that case, the Kafka
partitions will be devided between the instances.

# Compatibility

-   kafka >= 0.9
-   influxdb >= 1.2
-   go >= 1.8

