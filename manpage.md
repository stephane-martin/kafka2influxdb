% kafka2influxdb(1)
% Stéphane Martin
% March 2017

NAME
====

kafka2influxdb – pull metrics stored in Kafka topics and push them to InfluxDB

SYNOPSIS
========

| **kafka2influxdb** [**--config**=**CONFIGFILE**] **check-topics**
| **kafka2influxdb** [**--config**=**CONFIGFILE**] **check-config**
| **kafka2influxdb** [**--config**=**CONFIGFILE**] **ping-influxdb**
| **kafka2influxdb** [**--config**=**CONFIGFILE**] **start** [**--syslog**] [**--logfile**=**LOGFILE**]
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
:   Start to forward metrics

OPTIONS
=======

**-h**, **--help**
:   Display a friendly help message.

**--syslog**
:   Write log messages to syslog (messages will also be written to stdout)

**--logfile**=**LOGFILE**
:   Write log messages to LOGFILE instead of stdout


FILES
=====

*/etc/kafka2influxdb/kafka2influxdb.toml*
:   Configuration file

