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

# Installation

Just `go get github.com/stephane-martin/kafka2influxdb`

The dependencies are vendored.

# Debian packaging

You can build a package for Debian based distributions (systemd only).

First install go, then:

```
sudo apt-get install devscripts dh-systemd
git clone https://github.com/stephane-martin/kafka2influxdb.git"
cd kafka2influxdb && dpkg-buildpackage -us -uc -b && cd ..
```

-   The package installs a systemd service (`kafka2influxdb`) but does not enable
it and does not start the service at package installation.
-   The configuration file is `/etc/kafka2influxdb/kafka2influxdb.toml`.
-   Logs are put in `/var/log/kafka2influxdb/kafka2influxdb.log`.

# Configuration

See [the configuration example](https://github.com/stephane-martin/kafka2influxdb/blob/master/kafka2influxdb.example.toml).

The configuration directory can be specified by a commandline flag
`--config=XXX` (it defaults to `/etc/kafka2influxdb`).

In that directory, the configuration filename must be `kafka2influxdb.toml`.

# Running

See `kafka2influxdb --help`.

kafka2influxdb uses a Kafka consumer group. Throughput may be increased by
running multiple instances of kafka2influxdb. In that case, the Kafka
partitions will be devided between the instances.

# Compatibility

-   kafka >= 0.9
-   influxdb >= 1.2
-   go >= 1.8

