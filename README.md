# Punt

Punt is a tiny, lightweight, and straightforward daemon that parses, transforms and ships logs to Elasticsearch. Punt was designed and built to be a fast and viable alternative to Logstash, which means it has a focus on fitting into the ELK stack. Punt was built at [Discord](https://github.com/hammerandchisel) to serve as the core component in the middle of our logging pipeline.

## Features

- Simple Dynamic Configuration File
- Performance Driven Design
- UDP/TCP (both delimiter and octet based framing)
- Multiple cross-protocol servers at the same time
- TLS/SSL
- Supports JSON logs

## Design / Why Not Logstash?

When Discord originally started logging, we used a standard [ELK stack](https://www.elastic.co/webinars/introduction-elk-stack) setup. Initially this worked well for a low-volume of logs, however as our log volume grew (~750m log lines a day) Logstash quickly began to fall behind. As we spent more and more time tweaking and scaling Logstash/JVM/JRuby, we quickly realised it was not a long-term solution. Punt spawned out of a frustrating weekend dealing with constant Logstash lockups and JVM struggles.

Where Logstash aims for extreme configurability, Punt aims heavily at performance while attempting to remain configurable to some resonable degree. A side effect of this is Punt's expectation that you do most (or all) of the filtering on client machines, usually through something like rsyslog. Examples of Punts performance requirements are visible within the codebase, which contains a custom syslog parser/server implementation aimed at performance.

## Installation

### Go

To install Punt using the Go toolchain, simply

```sh
go get github.com/hammerandchisel/punt/cmd/puntd
```

### Package (Debian/Ubuntu)

Punt was designed to be installed as a package on debian systems, and thus includes a simple dpkg build script based on [fpm](https://github.com/jordansissel/fpm). To build a package simply:

```sh
cd packaging/
VERSION=0.0.1 ./build.sh
```

The package includes a simple upstart script.
