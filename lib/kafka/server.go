package kafka

import (
	"encoding/json"
	"time"

	"github.com/zorkian/kafka"
	"github.com/discordapp/punt/lib/syslog"
)

type Server struct {
	Messages chan syslog.SyslogData
	Broker* kafka.Broker
	FreightTopic string
}

type ServerConfig struct {
	FreightTopic string 
	Address string
}

type AudioMetrics struct {
	Timestamp int64 `json:"timestamp"`
	MediaSessionID string `json:"media_session_id"`
	User struct {
		Rtt interface{} `json:"rtt"`
		Protocol interface{} `json:"protocol"`
		ID string `json:"id"`
		AudioInbound struct {
			PacketsLost int `json:"packets_lost"`
			Ssrc int `json:"ssrc"`
			Jitter int `json:"jitter"`
			PacketsReceived int `json:"packets_received"`
			AudioLevel float64 `json:"audio_level"`
		} `json:"audio_inbound"`
		AudioOutbound struct {
			PacketsSent int `json:"packetsSent"`
			Ssrc int `json:"ssrc"`
			AudioLevel float64 `json:"audioLevel"`
		} `json:"audioOutbound"`
	} `json:"user"`
	Channel struct {
		ID string `json:"id"`
	} `json:"channel"`
	Server struct {
		Hostname string `json:"hostname"`
		ID string `json:"id"`
	} `json:"server"`
}

func NewServer(config ServerConfig, messages chan syslog.SyslogData) *Server {

	broker, err := kafka.NewBroker("kafka-default", []string{config.Address}, kafka.NewBrokerConf("discord-clerk"))
	if err != nil {
		panic(err)
	}

	server := Server{
		Messages: messages,
		FreightTopic: config.FreightTopic,
		Broker: broker,
	}
	return &server
}

func (s *Server) Start() {
	partitions, err := s.Broker.PartitionCount(s.FreightTopic)
	if err != nil {
		panic(err)
	}

	for partitionID := int32(0); partitionID < partitions; partitionID++ {
		go s.partitionReader(s.FreightTopic, partitionID)
	}
}

func (s* Server) partitionReader(topic string, partitionID int32) {
	conf := kafka.NewConsumerConf(topic, partitionID)
	consumer, err := s.Broker.Consumer(conf)
	if err != nil {
		panic(err)
	}
	for {
		msg, err := consumer.Consume()
		if err != nil {
			panic(err)
		}

		unpack := make(map[string]interface{})
		if err := json.Unmarshal(msg.Value, &unpack); err != nil {
			panic(err)
		}
		var audio_metrics AudioMetrics
		if err := json.Unmarshal([]byte(unpack["data"].(string)), &audio_metrics); err != nil {
			panic(err)
		}

		parts := map[string]interface{}{
			"priority":  0,
			"timestamp": time.Unix(audio_metrics.Timestamp, 0),
			"hostname":  "127.0.0.1",
			"tag":       "discord_voice_quality",
			"pid":       0,
			"content":   unpack["data"],
		}
		s.Messages <- parts
	}
}
