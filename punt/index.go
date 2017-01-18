package punt

import (
	"github.com/jeromer/syslogparser"
	"github.com/lollipopman/syslogd"
	"gopkg.in/olivere/elastic.v5"
	"log"
	"strings"
	"time"
)

type IndexConfig struct {
	Bind        string `json:"bind"`
	Cluster     string `json:"cluster"`
	Prefix      string `json:"prefix"`
	Type        string `json:"type"`
	DateFormat  string `json:"date_format"`
	Transformer struct {
		Name   string                 `json:"name"`
		Config map[string]interface{} `json:"config"`
	} `json:"transformer"`
}

type Index struct {
	Cluster *Cluster
	Config  IndexConfig

	transformer Transformer
	server      *syslogd.Server
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
	i.server = syslogd.NewServer()

	bindInfo := strings.SplitN(i.Config.Bind, ":", 2)

	// Default is to just bind to udp
	var err error
	if len(bindInfo) == 1 {
		err = i.server.ListenUDP(bindInfo[0])
	} else if bindInfo[0] == "tcp" {
		log.Panicf("TCP is unsupported at the moment")
		// err = i.server.ListenTCP(bindInfo[1])
	} else if bindInfo[0] == "udp" {
		err = i.server.ListenUDP(bindInfo[1])
	} else {
		log.Panicf("Invalid bind information: %v", bindInfo)
	}

	if err != nil {
		log.Panicf("Failed to bind: %v", err)
	}

	channel := make(chan syslogparser.LogParts)
	i.server.Start(channel)

	var payload map[string]interface{}

	for {
		select {
		case msg := <-channel:
			payload, err = i.transformer.Transform(msg)
			if err != nil {
				log.Printf("Failed to transform message `%v`: %v", msg, err)
			}

			timestamp := msg["timestamp"].(time.Time)
			indexString := i.Config.Prefix + timestamp.Format(i.Config.DateFormat)
			payload["@timestamp"] = timestamp.Format("2006-01-02T15:04:05+00:00")

			i.Cluster.Incoming <- elastic.NewBulkIndexRequest().Index(indexString).Type(i.Config.Type).Doc(payload)
		case <-i.exit:
			return
		}
	}
}

func (i *Index) handleIncomingMessage(msg syslogparser.LogParts) error {
	payload, err := i.transformer.Transform(msg)
	if err != nil {
		return err
	}

	return i.Cluster.Ingest(payload)
}
