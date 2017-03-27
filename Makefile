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

deb: manpage
	if ! dpkg -s devscripts > /dev/null 2>&1 ; then sudo apt-get install devscripts; fi
	if ! dpkg -s dh-systemd > /dev/null 2>&1 ; then sudo apt-get install dh-systemd; fi
	cp $(src_dir)/kafka2influxdb.1 $(src_dir)/debian
	dpkg-buildpackage -us -uc -b

manpage:
	pandoc $(src_dir)/manpage.md -s -t man -o $(src_dir)/kafka2influxdb.1

.PHONY: build install clean distclean deb bindata manpage

DEFAULT_GOAL := build

