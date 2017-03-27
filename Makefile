mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
src_dir := $(shell dirname $(mkfile_path))
GOPATH := /tmp/kafka2influxdb/_build
GO := GOPATH=$(GOPATH) go
NAME := github.com/stephane-martin
PROJECT := kafka2influxdb

build:
	mkdir -p $(GOPATH)/src/$(NAME)
	cp -a $(src_dir) $(GOPATH)/src/$(NAME)
	$(GO) build $(NAME)/$(PROJECT)

install:
	mkdir -p $(DESTDIR)/usr/bin
	cp $(src_dir)/$(PROJECT) $(DESTDIR)/usr/bin
	mkdir -p $(DESTDIR)/etc/kafka2influxdb
	cp $(src_dir)/kafka2influxdb.example.toml $(DESTDIR)/etc/kafka2influxdb/kafka2influxdb.toml

clean:
	rm -f $(src_dir)/$(PROJECT)
	rm -rf /tmp/kafka2influxdb

distclean: clean


.PHONY: build install clean distclean
DEFAULT_GOAL := build

