# Punt

Punt is a small and simple syslog server that parses, transforms, and ships logs to Elasticsearch. Punt was built to be a fast and lightweight alternative to logstash/fluentd, focusing on saving logs in a Kibana compatible format. Punt was developed at [Discord](https://github.com/hammerandchisel) to serve in the middle of our system logging infrastructure, and it happily handles many billions of log messages per day.
