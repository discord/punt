package punt

import (
	"context"
	"gopkg.in/olivere/elastic.v5"
	"log"
)

type ClusterConfig struct {
	URL        string `json:"url"`
	NumWorkers int    `json:"num_workers"`
	BulkSize   int    `json:"bulk_size"`
}

type Cluster struct {
	Config   ClusterConfig
	Incoming chan *elastic.BulkIndexRequest

	exit     chan bool
	workers  []*ClusterWorker
	esClient *elastic.Client
}

func NewCluster(config ClusterConfig) *Cluster {
	return &Cluster{
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

	// TODO: config this
	payload := make(map[string]interface{})
	index := make(map[string]interface{})
	index["number_of_replicas"] = 0
	payload["index"] = index

	ctx := context.Background()
	_, err = c.esClient.IndexPutSettings("_all").BodyJson(payload).Do(ctx)
	if err != nil {
		log.Printf("Fail: %v", err)
	}

	c.spawnWorkers()
	return nil
}

func (c *Cluster) Ingest(payload map[string]interface{}) error {
	return nil
}

func (c *Cluster) spawnWorkers() {
	for i := 0; i < c.Config.NumWorkers; i++ {
		c.workers = append(c.workers, NewClusterWorker(c))
		go c.workers[len(c.workers)-1].run()
	}
}

type ClusterWorker struct {
	Cluster *Cluster

	exit   chan bool
	esBulk *elastic.BulkService
}

func NewClusterWorker(cluster *Cluster) *ClusterWorker {
	return &ClusterWorker{
		Cluster: cluster,
		exit:    make(chan bool, 0),
	}
}

func (cw *ClusterWorker) run() {
	for {
		select {
		case request := <-cw.Cluster.Incoming:
			if cw.esBulk == nil {
				cw.esBulk = cw.Cluster.esClient.Bulk()
			}

			cw.esBulk.Add(request)

			if cw.esBulk.NumberOfActions() >= cw.Cluster.Config.BulkSize {
				cw.writeToElastic()
			}
		case <-cw.exit:
			return
		}
	}
}

func (cw *ClusterWorker) writeToElastic() {
	ctx := context.Background()
	_, err := cw.esBulk.Do(ctx)

	if err != nil {
		log.Printf("Failed to write to ES: %v", err)
	}

	cw.esBulk = nil
}
