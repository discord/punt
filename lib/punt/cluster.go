package punt

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/discordapp/punt/lib/syslog"
	"github.com/olivere/elastic"
)

type ClusterServerConfig struct {
	Type         string `json:"type"`
	Bind         string `json:"bind"`
	OctetCounted bool   `json:"octet_counted"`
	CertFile     string `json:"tls_cert_file"`
	KeyFile      string `json:"tls_key_file"`
}

type ClusterConfig struct {
	URL           string                 `json:"url"`
	NumWorkers    int                    `json:"num_workers"`
	BulkSize      int                    `json:"bulk_size"`
	Datastores    map[string]interface{} `json:"datastores"`
	Servers       []ClusterServerConfig  `json:"servers"`
	BufferSize    int                    `json:"buffer_size"`
	FlushInterval int                    `json:"flush_interval"`
	Debug         bool                   `json:"debug"`
}

type Cluster struct {
	Name     string
	State    *State
	Config   ClusterConfig
	Incoming chan *elastic.BulkIndexRequest

	metrics     *statsd.Client
	messages    chan syslog.SyslogData
	hostname    string
	reliability int
	exit        chan bool
	workers     []*ClusterWorker
}

func NewCluster(state *State, name string, config ClusterConfig) *Cluster {
	name, err := os.Hostname()
	if err != nil {
		log.Panicf("Failed to get hostname: %v", err)
	}

	return &Cluster{
		Name:     name,
		State:    state,
		Config:   config,
		Incoming: make(chan *elastic.BulkIndexRequest),
		metrics:  NewStatsdClient("punt", []string{fmt.Sprintf("cluster-name:%s", name)}),
		messages: make(chan syslog.SyslogData, config.BufferSize),
		hostname: name,
		exit:     make(chan bool),
		workers:  make([]*ClusterWorker, 0),
	}
}

func (c *Cluster) Run() error {
	log.Printf("Cluster %v is starting up", c.Name)

	// Spawn the index GC goroutine
	go c.pruneLoop()
	log.Printf("  successfully started prune worker")

	// Spawn the workers, which process and save incoming messages
	c.spawnWorkers()
	log.Printf("  successfully started %v workers", c.Config.NumWorkers)

	// Spawn servers, which recieve incoming messages and pass them to workers
	c.spawnServers()
	log.Printf("  completed startup")

	return nil
}

func (c *Cluster) spawnWorkers() {
	for i := 0; i < c.Config.NumWorkers; i++ {
		c.workers = append(c.workers, NewClusterWorker(c))
		go c.workers[len(c.workers)-1].run()
	}
}

func (c *Cluster) spawnServers() {
	for idx := range c.Config.Servers {
		serverConfig := c.Config.Servers[idx]
		c.startServer(serverConfig)
	}
}

func (c *Cluster) startServer(config ClusterServerConfig) {
	var syslogServer *syslog.Server

	errors := make(chan syslog.InvalidMessage)

	serverConfig := syslog.ServerConfig{
		TCPFrameMode: syslog.FRAME_MODE_DELIMITER,
		Format:       syslog.FORMAT_RFC3164,
	}

	// If we're using TCP and want to octect count, set that here
	if config.Type == "tcp" {
		if config.OctetCounted {
			serverConfig.TCPFrameMode = syslog.FRAME_MODE_OCTET_COUNTED
		}
	}

	syslogServer = syslog.NewServer(&serverConfig, c.messages, errors)

	// Loop over and consume error payloads, this has inherent backoff within the
	//  sending side as the channel is async-sent to.
	go func() {
		for err := range errors {
			log.Printf("Error reading incoming message (%v): %s (%v)", err.Error, err.Data, len(err.Data))
			c.metrics.Incr("msgs.error", []string{}, 1)
		}
	}()

	// Finally, we're ready to setup and start our syslog server
	var err error

	if config.Bind == "" {
		log.Panicf("Cannot bind to empty address")
	}

	if config.Type == "tcp" {
		var server net.Listener

		if config.CertFile != "" {
			cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)

			if err != nil {
				log.Panicf("Failed to load X509 TLS Certificate: %v", err)
			}

			tlsConfig := tls.Config{Certificates: []tls.Certificate{cert}}
			server, err = tls.Listen("tcp", config.Bind, &tlsConfig)
		} else {
			server, err = net.Listen("tcp", config.Bind)
		}
		syslogServer.AddTCPListener(server)
	} else if config.Type == "udp" {
		addr, err := net.ResolveUDPAddr("udp", config.Bind)

		if err != nil {
			log.Panicf("Failed to resolve UDP bind address: %v (%v)", err, config.Bind)
		}

		server, err := net.ListenUDP("udp", addr)
		syslogServer.AddUDPListener(server)
	} else {
		log.Panicf("Invalid server type: %v", config.Type)
	}

	if err != nil {
		log.Panicf("Failed to bind: %v", err)
	}

	log.Printf("  successfully started server %v:%v", config.Type, config.Bind)
}

func (c *Cluster) pruneLoop() {
	ticker := time.NewTicker(time.Duration(5) * time.Minute)

	for _ = range ticker.C {
		c.workers[0].ctrl <- "prune"
	}
}
