package punt

import (
	"gopkg.in/olivere/elastic.v5"
)

type Cluster struct {
	URL        string `json:"url"`
	NumWorkers int    `json:"num_workers"`
	BulkSize   uint   `json:"bulk_size"`

	exit     chan bool
	workers  []*ClusterWorker
	esClient *elastic.Client
}

func (c *Cluster) Run() error {
	client, err := elastic.NewClient(elastic.SetURL(c.URL))
	if err != nil {
		return err
	}
	c.esClient = client

	c.exit = make(chan bool, 1)
	c.spawnWorkers()
	return nil
}

func (c *Cluster) Ingest(payload map[string]interface{}) error {
	return nil
}

func (c *Cluster) spawnWorkers() {
	for i := 0; i < c.NumWorkers; i++ {
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
		exit:    make(chan bool, 1),
	}
}

func (cw *ClusterWorker) run() {

}
