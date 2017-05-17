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

func (m *Metrics) addTopicSuccessCount(topic string, points int64) {
	topicCount := m.topicCounts[topic]
	topicCount.successCount = m.topicCounts[topic].successCount + points
	m.topicCounts[topic] = topicCount
	m.Flush()
}

func (m *Metrics) addTopicFailureCount(topic string, points int64) {
	topicCount := m.topicCounts[topic]
	topicCount.failureCount = m.topicCounts[topic].failureCount + points
	m.topicCounts[topic] = topicCount
	m.Flush()
}

func (m *Metrics) addParseErrors(topic string, points int64) {
	m.parseErrors[topic] = m.parseErrors[topic] + points
	m.Flush()
}

func DbWritePoints(topic string, conf TopicCount) (point *influx.Point) {
	tags := map[string]string{
		"topic": topic,
	}

	fields := map[string]interface{}{
		"successCount": conf.successCount,
		"failureCount": conf.failureCount,
	}

	point, _ = influx.NewPoint("dbWritePoints", tags, fields, time.Now())
	return
}

func ParseErrors(topic string, totalErrors int64) (point *influx.Point) {
	tags := map[string]string{
		"topic": topic,
	}

	fields := map[string]interface{}{
		"totalErrors": totalErrors,
	}

	point, _ = influx.NewPoint("parsedErrors", tags, fields, time.Now())
	return
}

func (m *Metrics) Flush() {
	flush_duration := time.Millisecond * time.Duration(m.conf.MetricsConf.FlushInterval)
	if time.Now().Sub(m.last_push) > flush_duration {
		log.Debug("Flushing Metrics")
		bp, _ := influx.NewBatchPoints(
			influx.BatchPointsConfig{
				Database:        m.conf.MetricsConf.DatabaseName,
				Precision:       m.conf.MetricsConf.Precision,
				RetentionPolicy: m.conf.MetricsConf.RetentionPolicy,
			},
		)

		for topic, topicCount := range m.topicCounts {
			point := DbWritePoints(topic, topicCount)
			bp.AddPoint(point)
		}

		for topic, parseError := range m.parseErrors {
			point := ParseErrors(topic, parseError)
			bp.AddPoint(point)
		}

		client, err := m.conf.getMetricsInfluxHTTPClient()
		if err != nil {
			log.WithError(err).Error("Failed to create client")
		}
		client.Write(bp)
		m.last_push = time.Now()
	}
}