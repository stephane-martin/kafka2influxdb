# Motivation

InfluxDB is a time-series database. You can typically store metrics about your
servers and applications in InfluxDB.

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

In that case, Telegraf can push the metrics to a Kafka cluster.

kafka2finfluxdb provides a simple way to pull the metrics from Kafka and push
them to InfluxDB.

# Installation

Just `go get github.com/stephane-martin/kafka2influxdb`

The dependencies are vendored.

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


