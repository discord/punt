package kafka

import (
	"encoding/json"
	"log"
	"time"

	freight "github.com/discordapp/discord/discord_freight"
	"github.com/discordapp/punt/lib/syslog"
	"github.com/jpillora/backoff"
	"github.com/zorkian/kafka"
)

type Server struct {
	Messages chan syslog.SyslogData
	Errors   chan syslog.InvalidMessage
	Broker   *kafka.Broker
	Topic    string
}

type ServerConfig struct {
	Topic   string
	Address string
}

type MessageWithTimestamp struct {
	Timestamp int64 `json:"timestamp"`
}

func NewServer(config ServerConfig, messages chan syslog.SyslogData, errors chan syslog.InvalidMessage) *Server {
	broker, err := kafka.NewBroker(
		"kafka-default", []string{config.Address}, kafka.NewBrokerConf("discord-punt"))
	if err != nil {
		log.Panicf("Failed to create Kafka broker: %s", err)
	}

	return &Server{
		Messages: messages,
		Errors:   errors,
		Topic:    config.Topic,
		Broker:   broker,
	}
}

// Start begins reading from Kafka and processing messages.
func (s *Server) Start() {
	partitions, err := s.Broker.PartitionCount(s.Topic)
	if err != nil {
		log.Panicf("Could not get broker partition count: %v", err)
	}

	for partitionID := int32(0); partitionID < partitions; partitionID++ {
		go s.partitionReader(s.Topic, partitionID)
	}
}

func (s *Server) partitionReader(topic string, partitionID int32) {
	connectRetry := &backoff.Backoff{
		Min:    time.Second,
		Max:    time.Minute,
		Jitter: true,
	}

	messageRetry := &backoff.Backoff{
		Min:    time.Millisecond,
		Max:    time.Second,
		Jitter: true,
	}

	for {
		conf := kafka.NewConsumerConf(topic, partitionID)
		consumer, err := s.Broker.Consumer(conf)
		if err != nil {
			//s.Errors <- syslog.InvalidMessage{Data: "Failed to condume message", Error: err}
			log.Printf("Failed to connect to Kafka %s:%d: %s", topic, partitionID, err)
			time.Sleep(connectRetry.Duration())
			continue
		}
		connectRetry.Reset()

		for {
			msg, err := consumer.Consume()
			if err != nil {
				log.Printf("Failed to consume event from partition %d: %s", partitionID, err)
				time.Sleep(messageRetry.Duration())
				continue
			}

			// All messages should be Freight message formatted messages, unwrap that first
			// and by default use the SendTime of the message as the timestamp.
			var message freight.Message
			if err := json.Unmarshal(msg.Value, &message); err != nil {
				log.Printf("Failed to unmarshal wrapper: %s", err)
				time.Sleep(messageRetry.Duration())
				continue
			}
			timestamp := message.SendTime

			// Some messages contain a client submitted "timestamp" field. If that's provided,
			// use that as the timestamp.
			var report MessageWithTimestamp
			if err := json.Unmarshal([]byte(message.Data), &report); err == nil {
				// No error, use timestamp from client.
				timestamp = time.Unix(report.Timestamp, 0)
			}

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
}
