package main

import "github.com/influxdata/influxdb/models"
import influx "github.com/influxdata/influxdb/client/v2"
import "github.com/hashicorp/errwrap"

func parseLineProtocolPoint(message []byte, precision string) (*influx.Point, error) {
	points, err := models.ParsePoints(message)
	if err != nil {
		return nil, errwrap.Wrapf("Failed to parse line protocol metric from Kafka: {{err}}", err)
	}
	if len(points) >= 1 {
		return influx.NewPointFrom(points[0]), nil	
	}
	return nil, nil
}


