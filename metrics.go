package main

import (
	"time"
	"sync"
	influx "github.com/influxdata/influxdb/client/v2"
)

type Metrics struct {
	conf 			*GConfig
	topicCounts		map[string]TopicCount
	last_push		time.Time
	mutex 			sync.Mutex
}

type TopicCount struct {
	successCount	int64
	failureCount	int64
	parseCount	int64
	lock 		sync.Mutex
}

func (m *Metrics) IngestionCountMetric(topic string, points int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if conf, ok := m.topicCounts[topic]; ok {
		conf.successCount = conf.successCount + points
		m.topicCounts[topic] = conf
	} else {
		m.topicCounts[topic] = TopicCount{successCount: points}
	}
	m.Flush()
}

func (m *Metrics) IngestionFailureMetric(topic string, points int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if conf, ok := m.topicCounts[topic]; ok {
		conf.failureCount = conf.failureCount + points
		m.topicCounts[topic] = conf
	} else {
		m.topicCounts[topic] = TopicCount{failureCount: points}
	}
	m.Flush()
}

func (m *Metrics) ParsingErrorMetric(topic string, points int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if conf, ok := m.topicCounts[topic]; ok {
		conf.parseCount = conf.parseCount + points
		m.topicCounts[topic] = conf
	} else {
		m.topicCounts[topic] = TopicCount{parseCount: points}
	}
	m.Flush()
}

func (m *Metrics) addIngestionCount(bp influx.BatchPoints) {
	for topic, topicCount := range m.topicCounts {
		tags := map[string]string{
			"topic": topic,
		}
		for tag, value := range m.conf.MetricsConf.Tags {
			tags[tag] = value
		}
		fields := map[string]interface{}{
			"successCount": topicCount.successCount,
			"failureCount": topicCount.failureCount,
			"parseCount": topicCount.parseCount,
		}
		point, _ := influx.NewPoint("ingestion_count", tags, fields, time.Now())
		bp.AddPoint(point)
	}
}

func (m *Metrics) Flush() {
	flush_duration := time.Millisecond * time.Duration(m.conf.MetricsConf.FlushInterval)
	if m.conf.MetricsConf.Enabled && time.Now().Sub(m.last_push) > flush_duration {
		bp, _ := influx.NewBatchPoints(
			influx.BatchPointsConfig{
				Database:        m.conf.MetricsConf.DatabaseName,
				Precision:       m.conf.MetricsConf.Precision,
				RetentionPolicy: m.conf.MetricsConf.RetentionPolicy,
			},
		)
		m.addIngestionCount(bp)

		client, err := m.conf.getMetricsInfluxHTTPClient()
		if err != nil {
			log.WithError(err).Error("Failed to create metrics influx client")
		}
		client.Write(bp)
		m.last_push = time.Now()
	}
}
