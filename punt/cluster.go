package punt

import (
	"context"
	"crypto/tls"
	"gopkg.in/olivere/elastic.v5"
	"log"
	"net"
	"sync"
	"time"

	"../syslog"
)

type ClusterServerConfig struct {
	Type     string `json:"type"`
	Bind     string `json:"bind"`
	CertFile string `json:"tls_cert_file"`
	KeyFile  string `json:"tls_key_file"`
}

type ClusterConfig struct {
	URL            string              `json:"url"`
	NumWorkers     int                 `json:"num_workers"`
	BulkSize       int                 `json:"bulk_size"`
	CommitInterval int                 `json:"commit_interval"`
	BufferSize     int                 `json:"buffer_size"`
	Server         ClusterServerConfig `json:"server"`
	OctetCounted   bool                `json:"octet_counted"`
	Debug          bool                `json:"debug"`
}

type Cluster struct {
	Name     string
	State    *State
	Config   ClusterConfig
	Incoming chan *elastic.BulkIndexRequest

	server   *syslog.Server
	exit     chan bool
	workers  []*ClusterWorker
	esClient *elastic.Client
}

func NewCluster(state *State, name string, config ClusterConfig) *Cluster {
	return &Cluster{
		Name:     name,
		State:    state,
		Config:   config,
		Incoming: make(chan *elastic.BulkIndexRequest),
		exit:     make(chan bool),
		workers:  make([]*ClusterWorker, 0),
	}
}

func (c *Cluster) Run() error {
	client, err := elastic.NewClient(elastic.SetURL(c.Config.URL))
	if err != nil {
		return err
	}
	c.esClient = client

	// Set the global number of relicas to 0
	// TODO: config this
	payload := make(map[string]interface{})
	index := make(map[string]interface{})
	index["number_of_replicas"] = 0
	payload["index"] = index

	ctx := context.Background()
	_, err = c.esClient.IndexPutSettings("_all").BodyJson(payload).Do(ctx)
	if err != nil {
		log.Printf("Failed to set global replica count: %v", err)
	}

	c.spawnWorkers()
	go c.runServer()

	return nil
}

func (c *Cluster) spawnWorkers() {
	for i := 0; i < c.Config.NumWorkers; i++ {
		c.workers = append(c.workers, NewClusterWorker(c))
		go c.workers[len(c.workers)-1].run()
	}
}

func (c *Cluster) runServer() {
	messages := make(chan syslog.SyslogData, c.Config.BufferSize)
	errors := make(chan syslog.InvalidMessage)

	if c.Config.OctetCounted {
		c.server = syslog.NewServer(syslog.MODE_OCTET_COUNTED, messages, errors)
	} else {
		c.server = syslog.NewServer(syslog.MODE_DELIMITER, messages, errors)
	}

	// Debug errors
	go func() {
		for {
			err := <-errors
			log.Printf("Error reading incoming message (%v): %s", err.Error, err.Data)
		}
	}()

	var err error

	if c.Config.Server.Bind == "" {
		log.Panicf("Cannot bind to empty address")
	}

	if c.Config.Server.Type == "tcp" {
		var server net.Listener

		if c.Config.Server.CertFile != "" {
			cert, err := tls.LoadX509KeyPair(c.Config.Server.CertFile, c.Config.Server.KeyFile)

			if err != nil {
				log.Panicf("Failed to load X509 TLS Certificate: %v", err)
			}

			config := tls.Config{Certificates: []tls.Certificate{cert}}
			server, err = tls.Listen("tcp", c.Config.Server.Bind, &config)
		} else {
			server, err = net.Listen("tcp", c.Config.Server.Bind)
		}
		c.server.AddTCPListener(server)
	} else if c.Config.Server.Type == "udp" {
		addr, err := net.ResolveUDPAddr("udp", c.Config.Server.Bind)

		if err != nil {
			log.Panicf("Failed to resolve UDP bind address: %v (%v)", err, c.Config.Server.Bind)
		}

		server, err := net.ListenUDP("udp", addr)
		c.server.AddUDPListener(server)
	} else {
		log.Panicf("Invalid server type: %v", c.Config.Server.Type)
	}

	if err != nil {
		log.Panicf("Failed to bind: %v", err)
	}

	var payload map[string]interface{}

	log.Printf("Cluster %v is listening for syslog messages...", c.Name)

	for {
		select {
		case msg := <-messages:
			// Attempt to select the type based on the syslog tag
			var typ *Type

			typ = c.State.Types[msg["tag"].(string)]

			if typ == nil {
				typ = c.State.Types["*"]

				if typ == nil {
					log.Printf("Warning, recieved unhandled tag %v", msg["tag"].(string))
					continue
				}
			}

			// Grab this in case our transformer modifies it
			timestamp := msg["timestamp"].(time.Time)

			// Transform the message using the type properties
			payload, err = typ.Transformer.Transform(msg)
			if err != nil {
				log.Printf("Failed to transform message `%v`: %v", msg, err)
			}

			indexString := typ.Config.Prefix + timestamp.Format(typ.Config.DateFormat)
			payload["@timestamp"] = timestamp.Format("2006-01-02T15:04:05+00:00")

			if c.Config.Debug {
				log.Printf("(%v) %v", indexString, payload)
			}

			c.Incoming <- elastic.NewBulkIndexRequest().Index(indexString).Type(typ.Config.MappingType).Doc(payload)
		case <-c.exit:
			return
		}
	}
}

type ClusterWorker struct {
	Cluster *Cluster

	lock   *sync.RWMutex
	exit   chan bool
	esBulk *elastic.BulkService
}

func NewClusterWorker(cluster *Cluster) *ClusterWorker {
	return &ClusterWorker{
		Cluster: cluster,
		lock:    &sync.RWMutex{},
		exit:    make(chan bool, 0),
	}
}

func (cw *ClusterWorker) run() {
	cw.esBulk = cw.Cluster.esClient.Bulk()

	if cw.Cluster.Config.CommitInterval > 0 {
		go cw.commitLoop(cw.Cluster.Config.CommitInterval)
	}

	for {
		select {
		case request := <-cw.Cluster.Incoming:
			// Grab a read lock
			cw.lock.RLock()

			// Add our index request to the bulk request
			cw.esBulk.Add(request)

			// If we're above the bulk size request a commit (new goroutine to avoid deadlock)
			if cw.esBulk.NumberOfActions() >= cw.Cluster.Config.BulkSize {
				go cw.commit()
			}
			cw.lock.RUnlock()
		case <-cw.exit:
			return
		}
	}
}

func (cw *ClusterWorker) commitLoop(interval int) {
	timer := time.NewTicker(time.Duration(interval) * time.Second)

	for {
		<-timer.C

		cw.lock.RLock()
		if cw.esBulk.NumberOfActions() > 0 {
			go cw.commit()
		}
		cw.lock.RUnlock()
	}
}

func (cw *ClusterWorker) commit() {
	cw.lock.Lock()
	defer cw.lock.Unlock()

	if cw.esBulk.NumberOfActions() <= 0 {
		return
	}

	if cw.Cluster.Config.Debug {
		log.Printf("Commiting %v entries", cw.esBulk.NumberOfActions())
	}

	ctx := context.Background()
	_, err := cw.esBulk.Do(ctx)

	if err != nil {
		log.Printf("Failed to write to ES: %v", err)
	}

	cw.esBulk = cw.Cluster.esClient.Bulk()
}
