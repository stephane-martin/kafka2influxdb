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
	if [ -f /sbin/initctl ]; then \
		mkdir -p $(DESTDIR)/etc/init;\
		echo "manual" >> $(DESTDIR)/etc/init/kafka2influxdb.override;\
	fi
clean:
	rm -f $(src_dir)/$(PROJECT)
	rm -rf /tmp/kafka2influxdb

distclean: clean

deb:
	if ! dpkg -s devscripts > /dev/null 2>&1 ; then sudo apt-get install devscripts; fi
	if [ -f /sbin/initctl ]; then \
		cp -f $(src_dir)/kafka2influxdb.upstart $(src_dir)/debian;\
		cp -f $(src_dir)/debian/rules.nonsystemd $(src_dir)/debian/rules;\
		cp -f $(src_dir)/debian/control.nonsystemd $(src_dir)/debian/control;\
		sed -i 's/local\///' -i debian/kafka2influxdb.upstart;\
	elif [ -f /lib/systemd/systemd ];then \
		if ! dpkg -s dh-systemd > /dev/null 2>&1 ; then sudo apt-get install dh-systemd; fi;\
		cp -f $(src_dir)/kafka2influxdb.service $(src_dir)/debian;\
		cp -f $(src_dir)/debian/rules.systemd $(src_dir)/debian/rules;\
		cp -f $(src_dir)/debian/control.systemd $(src_dir)/debian/control;\
		sed -i 's/local\///' -i debian/kafka2influxdb.service;\
	else \
		cp -f $(src_dir)/kafka2influxdb.init $(src_dir)/debian;\
		cp -f $(src_dir)/debian/rules.nonsystemd $(src_dir)/debian/rules;\
		cp -f $(src_dir)/debian/control.nonsystemd $(src_dir)/debian/control;\
		sed -i 's/local\///' -i debian/kafka2influxdb.init;\
	fi
	dpkg-buildpackage -us -uc -b

manpage:
	pandoc $(src_dir)/manpage.md -s -t man -o $(src_dir)/docs/kafka2influxdb.1

bindata: manpage
	go-bindata kafka2influxdb.service kafka2influxdb.upstart kafka2influxdb.1

sync: bindata
	git add . && git commit && git pull origin master && git push origin master



.PHONY: build install clean distclean deb bindata manpage bindata sync

DEFAULT_GOAL := build

