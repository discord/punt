# Punt

Punt is a tiny syslog server that parses and saves messages in Elasticsearch. It was built as a fast and simple alternative to logstash/fluentd, and saves logs in a Kibana compatible format. Punt was developed at [Discord](https://github.com/hammerandchisel) to serve as the critical piece in our high-volume logging pipeline. Punt is very much a work-in-progress, and right now only supports build-time configuration.


## TODO

- SSL syslog support
- Runtime configuration (hot reloading as well)
- Queue/Backlog support (for ES issues)
