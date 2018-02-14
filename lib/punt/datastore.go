package punt

import (
	"log"
	"strings"
	"time"
)

type DatastorePayload struct {
	TypeName  string
	Timestamp time.Time
	Data      map[string]interface{}
}

func (dp *DatastorePayload) ReadDataPath(path string) interface{} {
	var current interface{}
	current = dp.Data

	for _, key := range strings.Split(path, ".") {
		current = current.(map[string]interface{})[key]
	}

	return current
}

// A datastore abstracts away the implementation details of a backing store for
//  logging records. Each ClusterWorker will have a single instance of each
//  configured datastore. Access to ALL functions within a datastore is guaraneteed
//  to be thread-local (e.g. no thread safety is required within the datastore
//  implementation)
type Datastore interface {
	// Called when the cluster worker starts up, should be used to initialize
	//  any external/database connections.
	Initialize() error

	// Called to write a single log line to the datastore. Errors returned are logged
	//  but do not effect any other operations.
	Write(*DatastorePayload) error

	// Called periodically to flush any batched/queued records. This can be ignored
	//  for datastores that do not implement batching/queueing.
	Flush() error

	// Called periodically to delete old data.
	Prune(string, time.Duration) error

	// Returns a list of all types this datastore cares about. If the list is empty
	//  the datastore will recieve payloads for all types. If it's nil the datastore
	//  wil recieve no payloads.
	GetSubscribedTypes() []string
}

type DatastoreBatcherDestination interface {
	Commit([]*DatastorePayload) error
}

type DatastoreBatcherConfig struct {
	BatchSize            int
	MaxRetries           int
	Backoff              int
	MaxConcurrentCommits int
}

// A batcher utilty for datastores
type DatastoreBatcher struct {
	config            *DatastoreBatcherConfig
	dest              DatastoreBatcherDestination
	bufferLength      int
	buffer            []*DatastorePayload
	concurrentCommits chan int8
}

func NewDatastoreBatcher(dest DatastoreBatcherDestination, config *DatastoreBatcherConfig) *DatastoreBatcher {
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
		bufferLength:      0,
		buffer:            nil,
		concurrentCommits: make(chan int8, config.MaxConcurrentCommits),
	}
}

func (db *DatastoreBatcher) Write(payload *DatastorePayload) error {
	if db.buffer == nil {
		db.buffer = make([]*DatastorePayload, db.config.BatchSize)
	}

	db.buffer[db.bufferLength] = payload
	db.bufferLength++

	// If the buffer is at our batch size, flush it
	if db.bufferLength >= db.config.BatchSize {
		db.Flush()
	}

	return nil
}

func (db *DatastoreBatcher) Flush() error {
	if db.bufferLength == 0 {
		return nil
	}

	// Take a local copy to the buffer
	buffer := db.buffer

	// Clear out the stored buffer / length
	db.buffer = nil
	db.bufferLength = 0

	// Run the commit function in a goroutine
	db.concurrentCommits <- 1
	go db.commit(buffer)

	return nil
}

func (db *DatastoreBatcher) commit(buffer []*DatastorePayload) {
	var err error

	// Clear our concurrent commit semaphore
	defer func() {
		<-db.concurrentCommits
	}()

	retries := db.config.MaxRetries

	for {
		err = db.dest.Commit(buffer)
		if err == nil {
			return
		}

		if retries <= 0 {
			break
		}

		time.Sleep(time.Duration(db.config.Backoff) * time.Millisecond)
		retries--
	}

	log.Printf("FAILED: DatastoreBatcher failed to commit: %v", err)
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
