# Punt

Punt is a tiny, lightweight, and straightforward daemon that parses, transforms and ships logs to Elasticsearch. Punt was designed and built to be a fast and viable alternative to Logstash, which means it has a focus on fitting into the ELK stack. Punt was built at [Discord](https://github.com/hammerandchisle) to serve as the core component in the middle of our logging pipeline.

## Features

- Simple Dynamic Configuration File
- Performance Driven Design
- UDP/TCP (both delimiter and octet based framing)
- Multiple cross-protocol servers at the same time
- TLS/SSL
- Supports JSON logs

## Installation

### Go

To install Punt using the Go toolchain, simply

```sh
go get github.com/hammerandchisel/punt
```


### Package (Debian/Ubuntu)

Punt was designed to be installed as a package on debian systems, and thus includes a simple dpkg build script based on [fpm](https://github.com/jordansissel/fpm). To build a package simply:

```sh
cd packaging/
VERSION=0.0.1 ./build.sh
```

The package includes a simple upstart script.

## Configuration

### Clusters

Punt supports shipping logs to multiple Elasticsearch clusters, while also supporting multiple input syslog servers. To aid in this design, Punts concept of clusters represent a single syslog server, and a single Elasticsearch cluster. For example, the following is a simple TCP/TLS/Octet-Counted cluster config and corresponding rsyslog config:

#### Cluster Config Fields

| Name | Description |
|------|-------------|
| url | The Elasticsearch connection URL |
| num\_workers | The number of Go-lang workers to use. Increasing this _can_ help reduce latency at high throughput |
| bulk\_size | The number of records to insert in bulk at a time. Increasing this will increase latency, but reduce work/thrasing on ES |
| commit\_interval | An interval (in seconds) at which to commit records. This can be used in place of bulk\_size to help reduce latency on low-throughput clusters |
| octet\_counted | Whether this cluster uses octet counting or newline delimiter for its TCP framing (if using UDP this is ignored) |
| debug | If true, this emits some useful (albeit verbose) log lines |
| server.type | TCP or UDP |
| server.bind | The server bind address to use |
| server.cert\_file | A TLS certificate to use for TCP-SSL |
| server.key\_file | A TLS key to use along with the certificate |

#### punt.json

```json
{
  "clusters": {
    "my-tcp-ssl-cluster": {
      "url": "http://localhost:9200",
      "num_workers": 4,
      "bulk_size": 500,
      "commit_interval": 60,
      "buffer_size": 24000,
      "server": {
        "type": "tcp",
        "tls_cert_file": "/tmp/mycert.crt",
        "tls_key_file": "/tmp/mycert.key",
        "bind": "localhost:1234"
      },
      "octet_counted": true
    }
  }
}
```

#### my-rsyslog.conf

```
action(
  type="omfwd"
  Target="my-tcp-ssl-cluster.website.corp"
  Port="1234"
  Protocol="tcp"
  StreamDriver="gtls"
  StreamDriverMode="1"
  StreamDriverAuthMode="x509/name"
  StreamDriverPermittedPeers="*.website.corp"
  TCP_Framing="octet-counted"
)
```

### Types

In Punt, types configure how to handle and process incoming log lines based on their syslog tag. By default, punt supports a wildcard type (`*`) which will be called for any syslog tags that do not explicitly have a type defined. The type configuration defines how (if at all) punt will process incoming log lines, and where the logs will end up. For example, the following is a configuration for an application which emits JSON logs:

#### Type Config Fields

| Name | Description |
|------|-------------|
| prefix | The index prefix to use for this type, this combined with a timestamp results in the index name |
| mapping\_type | This sets every log lines `type` field which can be used along with ES mappings |
| date\_format | The timestamp format to use, in Golangs standard date-format |
| transformer.name | The name of the transformer to use (direct, unpack-merge, unpack-take) |
| trasformer.config | Configuration for the transformer (currently unused) |

#### Type Transformers

| Name | Description |
|------|-------------|
| direct | This transformer simply inputs the log line as a normal syslog payload. This results in zero processing of the payload |
| unpack-merge | This transfomer first loads the log contents as JSON, and then merges these fields ontop of the syslog payload |
| unpack-take | This transfomer behaves the same as unpack-merge, but does not merge the fields and instead simply uses them as the payload |


#### punt.json

```json
{
  "types": {
    "app": {
      "prefix": "app-",
      "mapping_type": "app",
      "date_format": "2006.01.02.15",
      "transformer": {
        "name": "unpack-merge"
      }
    }
  }
}
```
