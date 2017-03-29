% kafka2influxdb(1)
% Stéphane Martin
% March 2017

NAME
====

kafka2influxdb – pull metrics stored in Kafka topics and push them to InfluxDB

SYNOPSIS
========

| **kafka2influxdb** [**--config**=**DIRECTORY**] **check-topics**
| **kafka2influxdb** [**--config**=**DIRECTORY**] **check-config**
| **kafka2influxdb** [**--config**=**DIRECTORY**] **ping-influxdb**
| **kafka2influxdb** [**--config**=**DIRECTORY**] **start** [start-options]
| **kafka2influxdb** **stop** [**pidfile**=**PIDFILE**]
| **kafka2influxdb** **install** [**--prefix**=**PREFIX**]
| **kafka2influxdb** **default-config**
| **kafka2influxdb** **--help**

DESCRIPTION
===========

**kafka2influxdb**:

| -   connects to a Kafka cluster
| -   selects some topics
| -   pulls metrics from the topics
| -   parses the metrics as JSON or InfluxDB Line Protocol
| -   writes the metrics to InfluxDB as batches

COMMANDS
========

**check-topics**
:   List the Kafka topics that will be selected for consuming

**default-config**
:   Print the default configuration

**check-config**
:   Check the configuration syntax

**ping-influxdb**
:   Check that InfluxDB is available

**start**
:   Start to push the metrics

**stop**
:   Stop kafka2influxdb

**install**
:   Copy the kafka2influxdb binary to the PATH, create a manual page an a
    service file

OPTIONS
=======

**--config**=**DIRECTORY**
:   The directory containing the configuration file `kafka2influxdb.toml`

**--daemonize**
:   Start kafka2influxdb in daemon mode

**--syslog**
:   Write log messages to syslog (messages will also be written to stdout)

**--loglevel**=**LEVEL**
:   Set logging level (info, debug, warn, error)

**--logfile**=**LOGFILE**
:   Write log messages to LOGFILE instead of stdout

**--pidfile**=**PIDFILE**
:   Write a PID file

**prefix**=**PREFIX**
:   Installation prefix

FILES
=====

*/etc/kafka2influxdb/kafka2influxdb.toml*
:   Configuration file

