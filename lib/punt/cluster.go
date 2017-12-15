package punt

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/discordapp/punt/lib/syslog"
	"gopkg.in/olivere/elastic.v5"
)

type ClusterServerConfig struct {
	Type         string `json:"type"`
	Bind         string `json:"bind"`
	OctetCounted bool   `json:"octet_counted"`
	CertFile     string `json:"tls_cert_file"`
	KeyFile      string `json:"tls_key_file"`
}

type ClusterReliabilityConfig struct {
	InsertRetries int `json:"insert_retries"`
	RetryDelay    int `json:"retry_delay"`
}

type ClusterConfig struct {
	URL            string                   `json:"url"`
	NumWorkers     int                      `json:"num_workers"`
	NumReplicas    *int                     `json:"num_replicas"`
	NumShards      *int                     `json:"num_shards"`
	BulkSize       int                      `json:"bulk_size"`
	CommitInterval int                      `json:"commit_interval"`
	Reliability    ClusterReliabilityConfig `json:"reliability"`
	Servers        []ClusterServerConfig    `json:"servers"`
	Debug          bool                     `json:"debug"`
	BufferSize     int                      `json:"buffer_size"`
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
	esClient    *elastic.Client
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

	client, err := elastic.NewClient(elastic.SetURL(c.Config.URL))
	if err != nil {
		return err
	}
	c.esClient = client

	// Set the number of repliacs globally
	payload := make(map[string]interface{})
	index := make(map[string]interface{})

	if c.Config.NumReplicas != nil {
		index["number_of_replicas"] = *c.Config.NumReplicas
	}

	if c.Config.NumShards != nil {
		index["number_of_shards"] = *c.Config.NumShards
	}

	payload["index"] = index

	ctx := context.Background()
	_, err = c.esClient.IndexPutSettings("_all").BodyJson(payload).Do(ctx)
	if err != nil {
		log.Printf("Failed to set global replica count: %v", err)
	}

	log.Printf("  successfully setup elasticsearch")

	// Spawn the index GC goroutine
	if len(c.State.Config.GC) > 0 {
		go c.gcIndexes()
		log.Printf("  successfully started Index GC routine")
	}

	// Spawn the workers, which process and save incoming messages
	c.spawnWorkers()
	log.Printf("  successfully started %v workers", c.Config.NumWorkers)

	// Spawn servers, which recieve incoming messages and pass them to workers
	c.spawnServers()
	log.Printf("  completed startup")

	return nil
}

func (c *Cluster) gcIndexes() {
	ticker := time.NewTicker(time.Minute * 15)

	for range ticker.C {
		for prefix, gcConfig := range c.State.Config.GC {
			GCIndexes(c.esClient, prefix, gcConfig)
		}
	}
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

type ClusterWorker struct {
	Cluster *Cluster

	name   string
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

	var err error
	var payload map[string]interface{}

	for {
		select {
		case msg := <-cw.Cluster.messages:
			// Increment the received metric
			tag := msg["tag"].(string)
			cw.Cluster.metrics.Incr("msgs.received", []string{fmt.Sprintf("tag:%s", tag)}, 1)

			// Attempt to select the type based on the syslog tag
			typ := cw.Cluster.State.Types[tag]

			if typ == nil {
				typ = cw.Cluster.State.Types["*"]

				if typ == nil {
					log.Printf("Warning, recieved unhandled tag %v", tag)
					cw.Cluster.metrics.Incr("msgs.unhandled", []string{fmt.Sprintf("tag:%s", tag)}, 1)
					continue
				}
			}

			// Grab this in case our transformer modifies it
			timestamp := msg["timestamp"].(time.Time)

			// Transform the message using the type properties
			payload, err = typ.Transformer.Transform(msg)
			if err != nil {
				log.Printf("Failed to transform message `%v`: %v", msg, err)
				cw.Cluster.metrics.Incr("msgs.failed", []string{fmt.Sprintf("tag:%s", tag)}, 1)
				continue
			}

			// Apply all the mutators
			for _, mutator := range typ.Mutators {
				mutator.Mutate(payload)
			}

			indexString := typ.Config.Prefix + timestamp.Format(typ.Config.DateFormat)
			payload["@timestamp"] = timestamp.Format("2006-01-02T15:04:05+00:00")
			payload["punt-server"] = cw.Cluster.hostname

			if cw.Cluster.Config.Debug {
				log.Printf("(%v) %v", indexString, payload)
			}

			// Distribute the message to all subscribers, using non-blocking send
			for _, sub := range typ.subscribers {
				select {
				case sub.channel <- payload:
				default:
				}
			}

			for _, alert := range typ.Alerts {
				alert.Run(payload)
			}

			// Grab a read lock
			cw.lock.RLock()

			// Add our index request to the bulk request
			cw.esBulk.Add(elastic.NewBulkIndexRequest().Index(indexString).Type(typ.Config.MappingType).Doc(payload))

			// If we're above the bulk size request a commit (new goroutine to avoid deadlock)
			if cw.esBulk.NumberOfActions() >= cw.Cluster.Config.BulkSize {
				go cw.commit()
			}
			cw.lock.RUnlock()

			cw.Cluster.metrics.Incr("msgs.processed", []string{fmt.Sprintf("tag:%s", tag)}, 1)
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

	actions := int64(cw.esBulk.NumberOfActions())
	if cw.Cluster.Config.Debug {
		log.Printf("Commiting %v entries", actions)
	}

	success := false
	attempts := 0

	for cw.Cluster.Config.Reliability.InsertRetries == -1 || cw.Cluster.Config.Reliability.InsertRetries > attempts {
		attempts += 1

		ctx := context.Background()
		_, err := cw.esBulk.Do(ctx)

		if err != nil {
			log.Printf("Failed to insert to ES (attempt #%v): %v", attempts, err)
			time.Sleep(time.Duration(cw.Cluster.Config.Reliability.RetryDelay) * time.Millisecond)
		} else {
			cw.Cluster.metrics.Count("msgs.inserted", actions, []string{}, 1)
			success = true
			break
		}
	}

	if !success {
		cw.Cluster.metrics.Count("msgs.dropped", actions, []string{}, 1)
	}

	cw.esBulk = cw.Cluster.esClient.Bulk()
}
