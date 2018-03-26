package kafka

import (
	"encoding/json"
	"time"
	"log"

	"github.com/zorkian/kafka"
	"github.com/discordapp/punt/lib/syslog"
)

type Server struct {
	Messages chan syslog.SyslogData
	Errors chan syslog.InvalidMessage
	Broker* kafka.Broker
	Topic string
}

type ServerConfig struct {
	Topic string 
	Address string
}

type Timestamp struct {
	Timestamp int64 `json:"timestamp"`
}

func NewServer(config ServerConfig, messages chan syslog.SyslogData, errors chan syslog.InvalidMessage) *Server {

	broker, err := kafka.NewBroker("kafka-default", []string{config.Address}, kafka.NewBrokerConf("discord-clerk"))
	if err != nil {
		panic(err)
	}

	server := Server{
		Messages: messages,
		Errors: errors,
		Topic: config.Topic,
		Broker: broker,
	}
	return &server
}

func (s *Server) Start() {
	partitions, err := s.Broker.PartitionCount(s.Topic)
	if err != nil {
		log.Panic("Could not get broker partition count: %v", err);
	}

	for partitionID := int32(0); partitionID < partitions; partitionID++ {
		go s.partitionReader(s.Topic, partitionID)
	}
}

func (s* Server) partitionReader(topic string, partitionID int32) {
	conf := kafka.NewConsumerConf(topic, partitionID)
	consumer, err := s.Broker.Consumer(conf)
	if err != nil {
		s.Errors <- syslog.InvalidMessage{Data: "Could not get Kafka consumer", Error: err}
	}
	for {
		msg, err := consumer.Consume()
		if err != nil {
			s.Errors <- syslog.InvalidMessage{Data: "Failed to condume message", Error: err}
		}

		unpack := make(map[string]interface{})
		if err := json.Unmarshal(msg.Value, &unpack); err != nil {
			s.Errors <- syslog.InvalidMessage{Data: "Invalid message received", Error: err}
		}
		var timestamp Timestamp
		if err := json.Unmarshal([]byte(unpack["data"].(string)), &timestamp); err != nil {
			s.Errors <- syslog.InvalidMessage{Data: "Could not get message timestamp", Error: err}
		}

		parts := map[string]interface{}{
			"priority":  0,
			"timestamp": time.Unix(timestamp.Timestamp, 0),
			"hostname":  "127.0.0.1",
			"tag":       "discord_voice_quality",
			"pid":       0,
			"content":   unpack["data"],
		}
		s.Messages <- parts
	}
}
