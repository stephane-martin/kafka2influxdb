package main

import (
	"time"
	influx "github.com/influxdata/influxdb/client/v2"
)

type Metrics struct {
	conf 			*GConfig
	topicCounts		map[string]TopicCount
	parseErrors		map[string]int64
	last_push		time.Time
}

type TopicCount struct {
	successCount	int64
	failureCount	int64
}

func (m *Metrics) IngestionCountMetric(topic string, points int64) {
	topicCount := m.topicCounts[topic]
	topicCount.successCount = m.topicCounts[topic].successCount + points
	m.topicCounts[topic] = topicCount
	m.Flush()
}

func (m *Metrics) IngestionFailureMetric(topic string, points int64) {
	topicCount := m.topicCounts[topic]
	topicCount.failureCount = m.topicCounts[topic].failureCount + points
	m.topicCounts[topic] = topicCount
	m.Flush()
}

func (m *Metrics) ParsingErrorMetric(topic string, points int64) {
	m.parseErrors[topic] = m.parseErrors[topic] + points
	m.Flush()
}

func (m *Metrics) addIngestionCount(bp influx.BatchPoints) {
	for topic, topicCount := range m.topicCounts {
		tags := map[string]string{
			"topic": topic,
		}
		fields := map[string]interface{}{
			"successCount": topicCount.successCount,
			"failureCount": topicCount.failureCount,
		}
		point, _ := influx.NewPoint("ingestion_count", tags, fields, time.Now())
		bp.AddPoint(point)
	}
}

func (m *Metrics) addParseErrors(bp influx.BatchPoints) {
	for topic, parseError := range m.parseErrors {
		tags := map[string]string{
			"topic": topic,
		}

		fields := map[string]interface{}{
			"totalErrors": parseError,
		}
		point, _ := influx.NewPoint("parsed_errors", tags, fields, time.Now())
		bp.AddPoint(point)
	}
	return
}

func (m *Metrics) Flush() {
	flush_duration := time.Millisecond * time.Duration(m.conf.MetricsConf.FlushInterval)
	if time.Now().Sub(m.last_push) > flush_duration {
		bp, _ := influx.NewBatchPoints(
			influx.BatchPointsConfig{
				Database:        m.conf.MetricsConf.DatabaseName,
				Precision:       m.conf.MetricsConf.Precision,
				RetentionPolicy: m.conf.MetricsConf.RetentionPolicy,
			},
		)
		m.addIngestionCount(bp)
		m.addParseErrors(bp)

		client, err := m.conf.getMetricsInfluxHTTPClient()
		if err != nil {
			log.WithError(err).Error("Failed to create metrics influx client")
		}
		client.Write(bp)
		m.last_push = time.Now()
	}
}