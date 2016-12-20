package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jeromer/syslogparser"
	"github.com/lollipopman/syslogd"
	"gopkg.in/olivere/elastic.v5"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
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

type Stats struct {
	Lines struct {
		Parsed struct {
			Success uint64 `json:"success"`
			Failure uint64 `json:"failure"`
		} `json:"parsed"`
		Written struct {
			Success uint64 `json:"success"`
			Failure uint64 `json:"failure"`
		} `json:"written"`
	} `json:"lines"`
}

func (s *Stats) Add(other *Stats) {
	s.Lines.Parsed.Success += other.Lines.Parsed.Success
	s.Lines.Parsed.Failure += other.Lines.Parsed.Failure
	s.Lines.Written.Success += other.Lines.Written.Success
	s.Lines.Written.Failure += other.Lines.Written.Failure
}

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
	// What to bind the debug server on
	debugBind string

	// Number of workers to run, defaults to (NUM_CORES - 1) / 2
	numWorkers int

	// Workers maping
	workers []*Worker

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

	numWorkers := (runtime.NumCPU() - 1) / 2

	return &State{
		debugBind:  "localhost:8878",
		numWorkers: numWorkers,
		workers:    make([]*Worker, numWorkers),
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
	stats  Stats
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
	var success bool

	w.state.wg.Add(1)

	for {
		select {
		case payload := <-w.state.in:
			success = w.handleMessage(payload.index, payload.parts)
			if success {
				w.stats.Lines.Parsed.Success += 1
			} else {
				w.stats.Lines.Parsed.Failure += 1
			}
		case <-w.exit:
			w.cleanup()
			w.state.wg.Done()
			return
		}
	}
}

func (w *Worker) handleMessage(index *IndexConfig, logParts syslogparser.LogParts) bool {
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
			return false
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

	return true
}

func (w *Worker) writeToElastic() {
	actions := uint64(w.bulk.NumberOfActions())
	ctx := context.Background()
	_, err := w.bulk.Do(ctx)

	if err != nil {
		fmt.Printf("Failed to write to elasticsearch: %v\n", err)
		w.stats.Lines.Written.Failure += actions
	} else {
		w.stats.Lines.Written.Success += actions
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

func handleDebugStats(state *State, c *net.TCPConn) {
	stats := new(Stats)

	for _, worker := range state.workers {
		stats.Add(&worker.stats)
	}

	data, err := json.Marshal(stats)
	if err != nil {
		fmt.Printf("Failed to serialize stats: %v\n", err)
		return
	}

	c.Write(append(data, '\n'))
}

func handleDebugConnection(state *State, c *net.TCPConn) {
	defer c.Close()
	fmt.Printf("Connection from %v established.\n", c.RemoteAddr())

	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if err != nil {
			fmt.Printf("Connection from %v closed: %v\n", c.RemoteAddr(), err)
			return
		}

		data := string(buf[:n])
		if strings.HasPrefix(data, "stats") {
			handleDebugStats(state, c)
		} else if strings.HasPrefix(data, "exit") || strings.HasPrefix(data, "quit") {
			return
		}
	}
}

func runDebugServer(state *State) {
	l, err := net.Listen("tcp", state.debugBind)
	if err != nil {
		fmt.Printf("Failed to start debug server: %v\n", err)
		return
	}
	ln := l.(*net.TCPListener)

	for {
		conn, err := ln.AcceptTCP()
		if err != nil {
			fmt.Printf("Failed to accept debug server connection: %v\n", err)
			return
		}

		go handleDebugConnection(state, conn)
	}

}

func run(state *State) {
	fmt.Printf("Starting up %v workers\n", state.numWorkers)
	for i := 0; i < state.numWorkers; i++ {
		w, err := NewWorker(state)
		if err != nil {
			fmt.Printf("Failed to start worker: %v\n", err)
			return
		}

		state.workers[i] = w
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
	for _, w := range state.workers {
		w.exit <- true
	}

	// Finally, wait for the workers to exit
	state.wg.Wait()
}

func main() {
	state := NewState()

	go runDebugServer(state)
	run(state)
}
