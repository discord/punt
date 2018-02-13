package punt

import (
	_ "github.com/olivere/elastic"
)

type ElasticsearchDatastore struct {
}

func NewElasticsearchDatastore(config map[string]interface{}) *ElasticsearchDatastore {
	return &ElasticsearchDatastore{}
}

func (e *ElasticsearchDatastore) Write(payload *DatastorePayload) error {
	return nil
}

func (e *ElasticsearchDatastore) Initialize() error {
	return nil
}

func (c *ElasticsearchDatastore) Flush() error {
	return nil
}
