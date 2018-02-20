package punt

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/bmhatfield/go-runtime-metrics/collector"
)

// New creates a new DogStatsD client.
func NewStatsdClient(prefix string, tags []string) *statsd.Client {
	// This will always work on localhost and DogStatsD is always localhost.
	s, _ := statsd.NewBuffered("127.0.0.1:8125", 100)
	s.Namespace = prefix + "."
	s.Tags = tags
	return s
}

func RunRuntimeCollector() {
	s, _ := statsd.NewBuffered("127.0.0.1:8125", 100)
	c := collector.New(func(key string, value uint64) {
		s.Gauge(key, float64(value), nil, 1)
	})

	c.EnableCPU = true
	c.EnableMem = true
	c.EnableGC = true
	c.Run()
}
