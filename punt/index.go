package punt

import (
	"github.com/jeromer/syslogparser"
	"github.com/lollipopman/syslogd"
	"log"
	"strings"
)

type IndexConfig struct {
	Bind        string `json:"bind"`
	Cluster     string `json:"cluster"`
	Prefix      string `json:"prefix"`
	Mapping     string `json:"mapping"`
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

func (i IndexConfig) getTransformer() Transformer {
	if i.Transformer.Name == "" || i.Transformer.Name == "direct" {
		return NewDirectTransformer(i.Transformer.Config)
	} else if i.Transformer.Name == "merge" {
		return NewUnpackMergeTransformer(i.Transformer.Config)
	} else if i.Transformer.Name == "take" {
		return NewUnpackTakeTransformer(i.Transformer.Config)
	} else {
		log.Panicf("Unknown parser type: %v", i.Transformer.Name)
		return nil
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

	for {
		select {
		case msg := <-channel:
			log.Printf("GOT MESSAGE %v\n", msg)
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
