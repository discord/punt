package datastore

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/mitchellh/mapstructure"
	"github.com/olivere/elastic"
)

type ElasticsearchConfig struct {
	URL      string
	Types    map[string]ElasticsearchTypeConfig
	Mappings map[string]ElasticsearchMappingConfig
	Batcher  DatastoreBatcherConfig
}

type ElasticsearchTypeConfig struct {
	Prefix      string
	DateFormat  string
	MappingType string
	Template    *struct {
		NumReplicas     *int     `json:"num_replicas"`
		NumShards       *int     `json:"num_shards"`
		RefreshInterval *string  `json:"refresh_interval"`
		Mappings        []string `json:"mappings"`
	} `json:"template"`
}

type ElasticsearchMappingFieldConfig struct {
	Type  string `json:"type"`
	Index bool   `json:"index"`
}

type ElasticsearchMappingConfig struct {
	Name   string                                     `json:"name"`
	Fields map[string]ElasticsearchMappingFieldConfig `json:"fields"`
	All    bool                                       `json:"all"`
}

type ElasticsearchDatastore struct {
	config   *ElasticsearchConfig
	esClient *elastic.Client
	batcher  *DatastoreBatcher
	metrics  *statsd.Client
	tags     []string
}

func NewElasticsearchDatastore(config map[string]interface{}) *ElasticsearchDatastore {
	es := &ElasticsearchDatastore{
		config: &ElasticsearchConfig{},
	}

	mapstructure.Decode(config, es.config)

	return es
}

func (e *ElasticsearchDatastore) Initialize(name string, metrics *statsd.Client) error {
	e.batcher = NewDatastoreBatcher(e, &e.config.Batcher, name, metrics)
	e.metrics = metrics
	e.tags = []string{fmt.Sprintf("datastore:%s", name)}

	client, err := elastic.NewClient(elastic.SetURL(e.config.URL))
	if err != nil {
		return err
	}
	e.esClient = client
	log.Printf("    connection to elasticsearch opened")

	e.syncMappings()
	log.Printf("    synced mapping types")

	e.syncIndexTemplates()
	log.Printf("    synced index templates")

	return nil
}

func (e ElasticsearchDatastore) GetSubscribedTypes() []string {
	if len(e.config.Types) == 0 {
		return nil
	}

	result := []string{}
	for typeName, _ := range e.config.Types {
		result = append(result, typeName)
	}

	return result
}

func (e *ElasticsearchDatastore) Write(payload *DatastorePayload) error {
	return e.batcher.Write(payload)
}

func (e *ElasticsearchDatastore) Flush() error {
	return e.batcher.Flush()
}

func (e *ElasticsearchDatastore) Prune(typeName string, keep int) error {
	typeConfig := e.config.Types[typeName]

	ctx := context.Background()
	indexes, err := elastic.NewIndicesGetService(e.esClient).Index(typeConfig.Prefix + "*").Do(ctx)

	if err != nil {
		return err
	}

	if len(indexes) <= keep {
		return nil
	}

	sortedIndexNames := make([]string, 0)
	for key, _ := range indexes {
		sortedIndexNames = append(sortedIndexNames, key)
	}
	sort.Strings(sortedIndexNames)

	toDelete := sortedIndexNames[:len(sortedIndexNames)-keep]

	var toDeleteBuff []string
	for len(toDelete) > 0 {
		if len(toDelete) > 25 {
			toDeleteBuff = toDelete[:25]
			toDelete = toDelete[25:]
		} else {
			toDeleteBuff = toDelete
			toDelete = make([]string, 0)
		}

		log.Printf("Deleting the following indexes for `%v`: %v", typeName, toDeleteBuff)
		_, err = elastic.NewIndicesDeleteService(e.esClient).Index(toDeleteBuff).Do(ctx)
		if err != nil {
			log.Printf("ERROR: failed to delete indexes (%v): %v", toDeleteBuff, err)
		}
	}

	return nil
}

func (e *ElasticsearchDatastore) Commit(typeName string, payloads []*DatastorePayload) error {
	bulk := e.esClient.Bulk()

	var indexString string
	typeConfig := e.config.Types[typeName]

	for _, payload := range payloads {
		indexString = typeConfig.Prefix + payload.Timestamp.Format(typeConfig.DateFormat)
		bulk.Add(elastic.NewBulkIndexRequest().Index(indexString).Type(typeConfig.MappingType).Doc(payload.Data))
	}

	ctx := context.Background()
	_, err := bulk.Do(ctx)
	if err == nil {
		e.metrics.Count("msgs.commited", int64(len(payloads)), e.tags, 1)
	}
	return err
}

func (e *ElasticsearchDatastore) syncIndexTemplates() {
	// Sync index templates
	for typeName, typeConfig := range e.config.Types {
		err := e.syncIndexTemplate(typeName, typeConfig)
		if err != nil {
			log.Printf("ERROR: failed to sync index template for type: %v (%v)", typeName, err)
		}
	}
}

func (e *ElasticsearchDatastore) syncIndexTemplate(typeName string, typeConfig ElasticsearchTypeConfig) error {
	if typeConfig.Template == nil {
		return nil
	}

	templateConfig := typeConfig.Template

	settings := make(map[string]interface{})
	if templateConfig.NumReplicas != nil {
		settings["number_of_replicas"] = templateConfig.NumReplicas
	}

	if templateConfig.NumShards != nil {
		settings["number_of_shards"] = templateConfig.NumShards
	}

	if templateConfig.RefreshInterval != nil {
		settings["refresh_interval"] = templateConfig.RefreshInterval
	}

	payload := make(map[string]interface{})
	payload["template"] = typeConfig.Prefix + "*"
	payload["settings"] = settings

	mappings := make(map[string]interface{})
	for _, mappingName := range templateConfig.Mappings {
		mappings[mappingName] = e.generateMappingJSON(e.config.Mappings[mappingName])
	}
	payload["mappings"] = mappings

	templateService := elastic.NewIndicesPutTemplateService(e.esClient)
	ctx := context.Background()
	_, err := templateService.BodyJson(payload).Name(typeConfig.Prefix + "template").Do(ctx)
	return err
}

func (e *ElasticsearchDatastore) syncMappings() {
	for mappingName, mappingConfig := range e.config.Mappings {
		e.syncMapping(mappingName, mappingConfig)
	}
}

func (e *ElasticsearchDatastore) generateMappingJSON(config ElasticsearchMappingConfig) map[string]interface{} {
	result := make(map[string]interface{})

	properties := make(map[string]interface{})
	for k, v := range config.Fields {
		properties[k] = map[string]interface{}{
			"type":  v.Type,
			"index": v.Index,
		}
	}

	result["properties"] = properties
	return result
}

func (e *ElasticsearchDatastore) syncMapping(name string, config ElasticsearchMappingConfig) {
	result := e.generateMappingJSON(config)
	ctx := context.Background()
	_, err := e.esClient.PutMapping().Type(name).BodyJson(result).Do(ctx)
	if err != nil {
		log.Printf("ERROR: failed to sync mapping %s: %v", name, err)
	}
}
