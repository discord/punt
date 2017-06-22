package punt

import (
	"github.com/DataDog/datadog-go/statsd"
)

// New creates a new DogStatsD client.
func NewStatsdClient(prefix string, tags []string) *statsd.Client {
	// This will always work on localhost and DogStatsD is always localhost.
	s, _ := statsd.NewBuffered("127.0.0.1:8125", 100)
	s.Namespace = prefix + "."
	s.Tags = tags
	return s
}
