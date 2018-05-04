package kafka

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/discordapp/punt/lib/syslog"
	"github.com/jpillora/backoff"
	kafka "github.com/zorkian/kafka-go"
)

const TICK_INTERVAL = 10 * time.Second

type Server struct {
	Messages chan syslog.SyslogData
	Errors   chan syslog.InvalidMessage
	Reader   *kafka.Reader
	Config   ServerConfig
}

type ServerConfig struct {
	Topic   string
	Address string
	GroupID string
}

type machineInfo struct {
	Hostname string `json:"hostname"`
}

type FreightMessage struct {
	Category    string      `json:"category"`
	MachineInfo machineInfo `json:"machine_info"`
	SendTime    time.Time   `json:"send_time"`
	Data        string      `json:"data"`
}

// NewServer constructs a Punt server that reads from Kafka.
func NewServer(
	config ServerConfig,
	messages chan syslog.SyslogData,
	errors chan syslog.InvalidMessage) *Server {

	return &Server{
		Messages: messages,
		Errors:   errors,
		Config:   config,
		Reader:   nil, // Not created until Start.
	}
}

// Start begins reading from Kafka and processing messages.
func (s *Server) Start() {
	s.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{s.Config.Address},
		GroupID:        s.Config.GroupID,
		Topic:          s.Config.Topic,
		MinBytes:       10 * 1024,        // 10KB
		MaxBytes:       10 * 1024 * 1024, // 10MB
		CommitInterval: 10 * time.Second, // At most we repeat 10 seconds of data on crash
		Logger:         log.New(os.Stderr, "", log.LstdFlags),
	})
	go s.readForever()
}

func (s *Server) readForever() {
	messageRetry := &backoff.Backoff{
		Min:    time.Millisecond,
		Max:    time.Second,
		Jitter: true,
	}

	msgCount := make(map[int]int)
	printTime := time.Now().Add(TICK_INTERVAL)

	for {
		msg, err := s.Reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Failed to consume message: %s", err)
			time.Sleep(messageRetry.Duration())
			continue
		}

		if time.Now().After(printTime) {
			printTime = time.Now().Add(TICK_INTERVAL)
			log.Printf("Kafka tick: handled messages from partitions: %+v", msgCount)
			msgCount = make(map[int]int)
		}
		msgCount[msg.Partition]++

		// All messages should be Freight message formatted messages, unwrap that first
		// and by default use the SendTime of the message as the timestamp.
		var message FreightMessage
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Printf("Failed to unmarshal wrapper: %s", err)
			time.Sleep(messageRetry.Duration())
			continue
		}
		timestamp := message.SendTime

		// Construct message format that the Punt system expects
		parts := map[string]interface{}{
			"priority":  0,
			"timestamp": timestamp,
			"hostname":  message.MachineInfo.Hostname,
			"tag":       message.Category,
			"pid":       0,
			"content":   message.Data,
		}
		s.Messages <- parts

		// Successfully stored one, reset backoff
		messageRetry.Reset()
	}
}
