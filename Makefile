mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
src_dir := $(shell dirname $(mkfile_path))
GOPATH := /tmp/kafka2influxdb/_build
GO := GOPATH=$(GOPATH) go

all: build

build:
	mkdir -p $(GOPATH)/src/github.com/stephane-martin
	cp -a $(src_dir) $(GOPATH)/src/github.com/stephane-martin
	$(GO) build github.com/stephane-martin/kafka2influxdb

install:
	mkdir -p $(DESTDIR)/usr/bin
	cp $(GOPATH)/src/github.com/stephane-martin/kafka2influxdb/kafka2influxdb $(DESTDIR)/usr/bin

