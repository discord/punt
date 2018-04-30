package datastore

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
)

var DroppedError error = errors.New("Dropped due to commit issue")

func WriteDataPath(data interface{}, path string, value interface{}) bool {
	var current interface{} = data
	var exists bool

	parts := strings.Split(path, ".")
	for _, key := range parts[:len(parts)-1] {
		current, exists = current.(map[string]interface{})[key]
		if !exists {
			return false
		}
	}

	current.(map[string]interface{})[parts[len(parts)-1]] = value
	return true
}

func ReadDataPath(data interface{}, path string) (interface{}, bool) {
	var current interface{} = data
	var exists bool

	for _, key := range strings.Split(path, ".") {
		current, exists = current.(map[string]interface{})[key]
		if !exists {
			return nil, false
		}
	}

	return current, true
}

type DatastorePayload struct {
	TypeName  string
	Timestamp time.Time
	Data      map[string]interface{}
}

func (dp *DatastorePayload) ReadDataPath(path string) (interface{}, bool) {
	return ReadDataPath(dp.Data, path)
}

// A datastore abstracts away the implementation details of a backing store for
//  logging records. Each ClusterWorker will have a single instance of each
//  configured datastore. Access to ALL functions within a datastore is guaraneteed
//  to be thread-local (e.g. no thread safety is required within the datastore
//  implementation)
type Datastore interface {
	// Called when the cluster worker starts up, should be used to initialize
	//  any external/database connections.
	Initialize(string, *statsd.Client) error

	// Called to write a single log line to the datastore. Errors returned are logged
	//  but do not effect any other operations.
	Write(*DatastorePayload) error

	// Called periodically to flush any batched/queued records. This can be ignored
	//  for datastores that do not implement batching/queueing.
	Flush() error

	// Called periodically to delete old data.
	Prune(string, int) error

	// Returns a list of all types this datastore cares about. If the list is empty
	//  the datastore will recieve payloads for all types. If it's nil the datastore
	//  wil recieve no payloads.
	GetSubscribedTypes() []string
}

type DatastoreBatcherDestination interface {
	Commit(string, []*DatastorePayload) error
}

type DatastoreBatcherConfig struct {
	BatchSize            int
	MaxRetries           int
	Backoff              int
	MaxConcurrentCommits int
	Drop                 bool
}

// A batcher utilty for datastores
type DatastoreBatcher struct {
	config            *DatastoreBatcherConfig
	dest              DatastoreBatcherDestination
	bufferLengths     map[string]int
	buffers           map[string][]*DatastorePayload
	concurrentCommits chan int8
	metrics           *statsd.Client
	tags              []string
}

func NewDatastoreBatcher(dest DatastoreBatcherDestination, config *DatastoreBatcherConfig, name string, metrics *statsd.Client) *DatastoreBatcher {
	if config.BatchSize <= 0 {
		log.Printf("WARNING: batchSize <= 0, setting to default of 1")
		config.BatchSize = 1
	}

	if config.MaxConcurrentCommits <= 0 {
		log.Printf("WARNING: MaxConcurrentCommits <= 0, setting to default of 3")
		config.MaxConcurrentCommits = 3
	}

	return &DatastoreBatcher{
		config:            config,
		dest:              dest,
		bufferLengths:     make(map[string]int),
		buffers:           make(map[string][]*DatastorePayload),
		concurrentCommits: make(chan int8, config.MaxConcurrentCommits),
		metrics:           metrics,
		tags:              []string{fmt.Sprintf("datastore:%s", name)},
	}
}

func (db *DatastoreBatcher) Write(payload *DatastorePayload) error {
	if buffer := db.buffers[payload.TypeName]; buffer == nil {
		db.buffers[payload.TypeName] = make([]*DatastorePayload, db.config.BatchSize)
		db.bufferLengths[payload.TypeName] = 0
	}

	length := db.bufferLengths[payload.TypeName]
	db.buffers[payload.TypeName][length] = payload
	db.bufferLengths[payload.TypeName] = length + 1

	// If the buffer is at our batch size, flush it
	if length+1 >= db.config.BatchSize {
		return db.flush(payload.TypeName)
	}

	return nil
}

func (db *DatastoreBatcher) Flush() error {
	for typeName, buffer := range db.buffers {
		if buffer == nil {
			continue
		}

		err := db.flush(typeName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DatastoreBatcher) flush(typeName string) error {
	if db.bufferLengths[typeName] == 0 {
		return nil
	}

	// Take a local copy to the buffer
	length := db.bufferLengths[typeName]
	buffer := db.buffers[typeName][:length]

	// Clear out the stored buffer / length
	db.buffers[typeName] = nil
	db.bufferLengths[typeName] = 0

	// If we're in "drop" mode, do a non blocking commit
	if db.config.Drop {
		select {
		case db.concurrentCommits <- 1:
			// Run the commit function in a goroutine
			go db.commit(typeName, buffer)
			return nil
		default:
			db.metrics.Count("msgs.dropped", int64(len(buffer)), append(db.tags, "reason:backoff"), 1)
			return DroppedError
		}
	} else {
		db.concurrentCommits <- 1
		go db.commit(typeName, buffer)
		return nil
	}
}

func (db *DatastoreBatcher) commit(typeName string, buffer []*DatastorePayload) {
	var err error

	// Clear our concurrent commit semaphore
	defer func() {
		<-db.concurrentCommits
	}()

	retries := db.config.MaxRetries

	for {
		err = db.dest.Commit(typeName, buffer)
		if err == nil {
			return
		}

		db.metrics.Incr("datastore.errors", db.tags, 1)

		if retries <= 0 {
			break
		}

		time.Sleep(time.Duration(db.config.Backoff) * time.Millisecond)
		retries--
	}

	db.metrics.Count("msgs.dropped", int64(len(buffer)), append(db.tags, "reason:datastore_error"), 1)
	log.Printf("FAILED: DatastoreBatcher failed to commit %v: %v", typeName, err)
}

func CreateDatastore(datastoreType string, datastoreConfig map[string]interface{}) Datastore {
	switch datastoreType {
	case "elasticsearch":
		return NewElasticsearchDatastore(datastoreConfig)
	case "clickhouse":
		return NewClickhouseDatastore(datastoreConfig)
	default:
		log.Panicf("Unknown datastore type: `%s`", datastoreType)
	}

	return nil
}
