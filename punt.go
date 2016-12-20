package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jeromer/syslogparser"
	"github.com/lollipopman/syslogd"
	"gopkg.in/olivere/elastic.v5"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"
)

const (
	// Just save the structured syslog message
	METHOD_NONE = iota

	// Parse the message as JSON and merge it into the syslog struct
	METHOD_UNPACK_MERGE

	// Just parse the message as JSON and save it
	METHOD_UNPACK_TAKE
)

// Represents a single log line with an index destination
type Payload struct {
	index *IndexConfig
	parts syslogparser.LogParts
}

func NewPayload(index *IndexConfig, parts syslogparser.LogParts) *Payload {
	return &Payload{
		index: index,
		parts: parts,
	}
}

// Index configuration determines how log lines are saved
type IndexConfig struct {
	// Index prefix (goes before date string)
	prefix string

	// Index date string format
	dateFormat string

	// Message handling method
	messageMethod int
}

func NewIndexConfig(prefix, dateFormat string, messageMethod int) *IndexConfig {
	return &IndexConfig{
		prefix:        prefix,
		dateFormat:    dateFormat,
		messageMethod: messageMethod,
	}
}

type State struct {
	// Number of workers to run, defaults to (NUM_CORES - 1) / 2
	numWorkers int

	// Incoming queue that workers pull off
	in chan *Payload

	// Wait group used for cleanly exiting workers
	wg sync.WaitGroup

	// ES connection string
	esURL string

	// Prefix Mapping
	indexes map[string]*IndexConfig

	// How many entries to queue up in one ES call
	bulkSize int
}

func NewState() *State {
	indexes := make(map[string]*IndexConfig)

	indexes["0.0.0.0:5140"] = NewIndexConfig("api-logs-", "2006.01.02.15", METHOD_UNPACK_MERGE)
	indexes["0.0.0.0:5141"] = NewIndexConfig("syslog-", "2006.01.02.15", METHOD_NONE)
	indexes["0.0.0.0:5142"] = NewIndexConfig("cassandra-", "2006.01.02.15", METHOD_NONE)

	return &State{
		numWorkers: (runtime.NumCPU() - 1) / 2,
		in:         make(chan *Payload, 0),
		esURL:      "http://127.0.0.1:9200",
		indexes:    indexes,
		bulkSize:   500,
	}
}

type Worker struct {
	state  *State
	exit   chan bool
	client *elastic.Client
	bulk   *elastic.BulkService
}

func NewWorker(state *State) (*Worker, error) {
	client, err := elastic.NewClient(elastic.SetURL(state.esURL))
	if err != nil {
		return nil, err
	}

	return &Worker{
		state:  state,
		exit:   make(chan bool, 0),
		client: client,
	}, nil
}

func (w *Worker) Run() {
	w.state.wg.Add(1)

	for {
		select {
		case payload := <-w.state.in:
			w.handleMessage(payload.index, payload.parts)
		case <-w.exit:
			w.cleanup()
			w.state.wg.Done()
			return
		}
	}
}

func (w *Worker) handleMessage(index *IndexConfig, logParts syslogparser.LogParts) {
	if w.bulk == nil {
		w.bulk = w.client.Bulk()
	}

	var data map[string]interface{}

	if index.messageMethod == METHOD_NONE {
		data = logParts
	} else {
		// Parse the message out into an interface
		err := json.Unmarshal([]byte(logParts["content"].(string)), &data)

		if err != nil {
			fmt.Printf("Failed to unmarshal data: %v (%v)\n", err, logParts["content"].(string))
			return
		}

		if index.messageMethod == METHOD_UNPACK_MERGE {
			// Take keys from the original syslog message and merge them into the parsed data
			for k, v := range logParts {
				if k == "content" {
					continue
				}

				if _, ok := data[k]; !ok {
					data[k] = v
				}
			}
		}
	}

	timestamp := logParts["timestamp"].(time.Time)

	// Format the syslog timestamp into the index date format, with prefix
	indexString := index.prefix + timestamp.Format(index.dateFormat)

	// Add the logstash @timestamp field in the correct format
	data["@timestamp"] = timestamp.Format("2006-01-02T15:04:05+00:00")

	// Add it to our bulk request
	w.bulk.Add(elastic.NewBulkIndexRequest().Index(indexString).Type("testing").Doc(data))

	if w.bulk.NumberOfActions() >= w.state.bulkSize {
		w.writeToElastic()
	}
}

func (w *Worker) writeToElastic() {
	ctx := context.Background()
	_, err := w.bulk.Do(ctx)

	if err != nil {
		fmt.Printf("Failed to write to elasticsearch: %v\n", err)
	}

	w.bulk = nil
}

func (w *Worker) cleanup() {
	w.writeToElastic()
}

func runIndex(state *State, hoststring string, index *IndexConfig, exit chan bool) {
	var err error

	syslogServer := syslogd.NewServer()

	err = syslogServer.ListenUDP(hoststring)
	if err != nil {
		fmt.Printf("Failed to start syslog server: %v\n", err)
		return
	}

	channel := make(chan syslogparser.LogParts)

	syslogServer.Start(channel)

	for {
		select {
		case msg := <-channel:
			state.in <- NewPayload(index, msg)
		case <-exit:
			return
		}
	}
}

func run(state *State) {
	workers := make([]*Worker, state.numWorkers)

	fmt.Printf("Starting up %v workers\n", runtime.NumCPU()-1)
	for i := 0; i < state.numWorkers; i++ {
		w, err := NewWorker(state)
		if err != nil {
			fmt.Printf("Failed to start worker: %v\n", err)
			return
		}

		workers[i] = w
		go w.Run()
	}

	exit := make(chan bool, 0)

	// Bind to all our index hoststring mappings
	for k, index := range state.indexes {
		go runIndex(state, k, index, exit)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)
	signal := <-signals

	fmt.Printf("Caught signal %s, exiting...\n", signal)

	// First, close all our syslog servers
	for _ = range state.indexes {
		exit <- true
	}

	// Next signal the workers to stop
	for _, w := range workers {
		w.exit <- true
	}

	// Finally, wait for the workers to exit
	state.wg.Wait()
}

func main() {
	state := NewState()

	run(state)
}
