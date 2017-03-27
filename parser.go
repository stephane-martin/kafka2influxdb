package main

import "fmt"
import "strconv"
import "strings"
import "reflect"
import "encoding/json"

import influx "github.com/influxdata/influxdb/client/v2"
import "github.com/influxdata/influxdb/models"
import "github.com/hashicorp/errwrap"

const (
	BOOLEAN MetricValueType = iota
	STRING
	NUMERIC
)

type Point struct {
	Timestamp int64
	Name      string
	Tags      map[string]string
	Fields    map[string]MetricValue
}

type MetricValueType int

type MetricValue struct {
	number  float64
	boolean bool
	str     string
	typ     MetricValueType
}

type Parser interface {
	Parse(message []byte) (*influx.Point, error)
}

func NewParser(format string, precision string) (Parser, error) {
	if format == "json" {
		return JsonParser{precision: precision}, nil
	} else if format == "influx" {
		return LineProtocolParser{precision: precision}, nil
	} else {
		return nil, fmt.Errorf("Unknown format for points in Kafka")
	}
}

type JsonParser struct {
	precision string
}

func (p JsonParser) Parse(message []byte) (*influx.Point, error) {
	var point Point
	err := json.Unmarshal(message, &point)
	if err != nil {
		return nil, errwrap.Wrapf("Failed to parse the JSON encoded metric from Kafka: {{err}}", err)
	}
	t, err := models.SafeCalcTime(point.Timestamp, p.precision)
	if err != nil {
		return nil, errwrap.Wrapf("The metric timestamp is out of range", err)
	}
	influxPoint, err := influx.NewPoint(point.Name, point.Tags, point.getFields(), t)
	if err != nil {
		return nil, errwrap.Wrapf("The metric from Kafka is not valid: {{err}}", err)
	}
	return influxPoint, nil
}

type LineProtocolParser struct {
	precision string
}

func (p LineProtocolParser) Parse(message []byte) (*influx.Point, error) {
	points, err := models.ParsePoints(message)
	if err != nil {
		return nil, errwrap.Wrapf("Failed to parse line protocol metric from Kafka: {{err}}", err)
	}
	if len(points) >= 1 {
		return influx.NewPointFrom(points[0]), nil	
	}
	return nil, nil
}


func (p *Point) getFields() map[string]interface{} {
	m := map[string]interface{}{}
	for k, v := range p.Fields {
		m[k] = v.getValue()
	}
	return m
}

func (p Point) String() string {
	res := p.Name
	res += "\n"
	res += strconv.FormatInt(p.Timestamp, 10)
	res += "\n"
	res += "Tags: "
	tags := []string{}
	for tagname, tagvalue := range p.Tags {
		tags = append(tags, tagname+": "+tagvalue)
	}
	res += strings.Join(tags, ", ")
	res += "\n"
	res += "Fields: "
	fields := []string{}
	for fieldname, fieldvalue := range p.Fields {
		fields = append(fields, fieldname+": "+fieldvalue.String())
	}
	res += strings.Join(fields, ", ")
	res += "\n"
	return res
}

func (p MetricValue) String() string {
	if p.typ == BOOLEAN {
		if p.boolean {
			return "true"
		} else {
			return "false"
		}
	} else if p.typ == NUMERIC {
		return strconv.FormatFloat(p.number, 'f', -1, 64)
	} else {
		return p.str
	}
}

func (p *MetricValue) getValue() interface{} {
	if p.typ == BOOLEAN {
		return p.boolean
	} else if p.typ == NUMERIC {
		return p.number
	} else {
		return p.str
	}
}

func (p *MetricValue) UnmarshalJSON(b []byte) (err error) {
	var num_val float64 = 0.0
	boolean_val := false
	str_val := ""

	p.number = 0
	p.boolean = false
	p.str = ""
	p.typ = 0

	// check if the slice represents a boolean
	err = json.Unmarshal(b, &boolean_val)
	if err == nil {
		p.boolean = boolean_val
		p.typ = BOOLEAN
		return nil
	}
	if _, ok := err.(*json.SyntaxError); ok {
		return err
	}

	// check if the slice represents a float
	err = json.Unmarshal(b, &num_val)
	if err == nil {
		p.number = num_val
		p.typ = NUMERIC
		return nil
	}
	if _, ok := err.(*json.SyntaxError); ok {
		return err
	}

	// check if the slice represents a string
	err = json.Unmarshal(b, &str_val)
	if err == nil {
		p.str = str_val
		p.typ = STRING
		return nil
	}
	if _, ok := err.(*json.SyntaxError); ok {
		return err
	}

	if unmarshal_err, ok := err.(*json.UnmarshalTypeError); ok {
		unmarshal_err.Type = reflect.TypeOf(*p)
		return unmarshal_err
	}

	return err
}

