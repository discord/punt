package punt

import (
	"crypto/tls"
	"gopkg.in/olivere/elastic.v5"
	"log"
	"net"
	"strings"
	"time"

	"../syslog"
)

type IndexConfig struct {
	Bind        string `json:"bind"`
	CertFile    string `json:"tls_cert_file"`
	KeyFile     string `json:"tls_key_file"`
	Cluster     string `json:"cluster"`
	Prefix      string `json:"prefix"`
	Type        string `json:"type"`
	DateFormat  string `json:"date_format"`
	BufferSize  int    `json:"buffer_size"`
	Transformer struct {
		Name   string                 `json:"name"`
		Config map[string]interface{} `json:"config"`
	} `json:"transformer"`
}

type Index struct {
	Cluster *Cluster
	Config  IndexConfig

	transformer Transformer
	server      *syslog.Server
	exit        chan bool
}

func NewIndex(config IndexConfig, cluster *Cluster) *Index {
	return &Index{
		Cluster:     cluster,
		Config:      config,
		transformer: GetTransformer(config.Transformer.Name, config.Transformer.Config),
		exit:        make(chan bool),
	}
}

func (i *Index) Run() {
	messages := make(chan syslog.SyslogData, i.Config.BufferSize)
	errors := make(chan syslog.InvalidMessage)

	i.server = syslog.NewServer(messages, errors)

	// Debug errors
	go func() {
		for {
			err := <-errors
			log.Printf("Error reading incoming message (%v): %s", err.Error, err.Data)
		}
	}()

	bindInfo := strings.SplitN(i.Config.Bind, ":", 2)

	// Default is to just bind to udp
	var err error

	if len(bindInfo) == 2 {
		if bindInfo[0] == "tcp" {
			var server net.Listener

			if i.Config.CertFile != "" {
				cert, err := tls.LoadX509KeyPair(i.Config.CertFile, i.Config.KeyFile)

				if err != nil {
					log.Panicf("Failed to load X509 TLS Certificate: %v", err)
				}

				config := tls.Config{Certificates: []tls.Certificate{cert}}
				server, err = tls.Listen("tcp", bindInfo[1], &config)
			} else {
				server, err = net.Listen("tcp", bindInfo[1])
			}
			i.server.AddTCPListener(server)
		} else if bindInfo[0] == "udp" {
			addr, err := net.ResolveUDPAddr("udp", bindInfo[1])

			if err != nil {
				log.Panicf("Failed to resolve UDP bind address: %v (%v)", err, bindInfo[1])
			}

			server, err := net.ListenUDP("udp", addr)
			i.server.AddUDPListener(server)
		} else {
			log.Panicf("Invalid bind type: %v", bindInfo[0])
		}
	} else {
		log.Panicf("Invalid bind information: %v", bindInfo)
	}

	if err != nil {
		log.Panicf("Failed to bind: %v", err)
	}

	var payload map[string]interface{}

	log.Printf("Index %v is ready and waiting for messages", i.Config.Prefix)
	for {
		select {
		case msg := <-messages:
			// Grab this in case our transformer modifies it
			timestamp := msg["timestamp"].(time.Time)

			payload, err = i.transformer.Transform(msg)
			if err != nil {
				log.Printf("Failed to transform message `%v`: %v", msg, err)
			}

			indexString := i.Config.Prefix + timestamp.Format(i.Config.DateFormat)
			payload["@timestamp"] = timestamp.Format("2006-01-02T15:04:05+00:00")

			i.Cluster.Incoming <- elastic.NewBulkIndexRequest().Index(indexString).Type(i.Config.Type).Doc(payload)
		case <-i.exit:
			return
		}
	}
}

func (i *Index) handleIncomingMessage(msg syslog.SyslogData) error {
	payload, err := i.transformer.Transform(msg)
	if err != nil {
		return err
	}

	return i.Cluster.Ingest(payload)
}
