# Punt

Punt is a lightweight and simple daemon that parses, transforms, mutates, and ships logs into Elasticsearch. Punt was built as a fast and reliable alternative to Logstash, which means it's focus is to fit directly into existing ELK setups. Punt was built at [Discord](https://github.com/hammerandchisle) to manage the over 2.5 billion log lines we process per day.

## Features

- Designed to be fast and reliable
- Simple JSON-based configuration file
- Supports rsyslog over UDP/TCP, including multiple framing formats and SSL
- Multiple ingest servers and egress ES clusters
- Ability to parse and transform structured (JSON) logs


## Why Not Logstash?

When Discord originally started logging, we used a standard [ELK stack](https://www.elastic.co/webinars/introduction-elk-stack) setup. Initially this worked well for a low-volume of logs, however as our log volume grew (~750m log lines a day) Logstash quickly began to fall behind. As we spent more and more time tweaking and scaling Logstash/JVM/JRuby, we quickly realised it was not a long-term solution. Punt spawned out of a frustrating weekend dealing with constant Logstash lockups and JVM struggles.

Where Logstash aims to be immensely configurable and pluggable via its DSL, Punt aims to be an extremely performant solution, without compromising or reducing the core features required to handle and store structured log data.

## Installation

### Go

To install Punt using the Go toolchain, simply

```sh
go get github.com/discordapp/punt/cmd/puntd
```

### Package (Debian/Ubuntu)

Punt was designed to be installed as a package on debian systems, and thus includes a simple dpkg build script based on [fpm](https://github.com/jordansissel/fpm). To build a package simply:

```sh
cd packaging/
VERSION=0.0.1 ./build.sh
```

The package includes a simple upstart script.
