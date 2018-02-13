package punt

import (
	"context"
	"log"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/olivere/elastic"
)

type ElasticsearchConfig struct {
	URL      string
	Types    map[string]ElasticsearchTypeConfig
	Mappings map[string]ElasticsearchMappingConfig
	DatastoreBatcherConfig
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
}

func NewElasticsearchDatastore(config map[string]interface{}) *ElasticsearchDatastore {
	es := &ElasticsearchDatastore{
		config: &ElasticsearchConfig{},
	}

	mapstructure.Decode(config, es.config)

	es.batcher = NewDatastoreBatcher(es, &es.config.DatastoreBatcherConfig)
	return es
}

func (e *ElasticsearchDatastore) Initialize() error {
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

func (e *ElasticsearchDatastore) Write(payload *DatastorePayload) error {
	return e.batcher.Write(payload)
}

func (e *ElasticsearchDatastore) Flush() error {
	return e.batcher.Flush()
}

func (e *ElasticsearchDatastore) Prune(typeName string, keepDuration time.Duration) error {
	return nil
}

func (e *ElasticsearchDatastore) Commit(payloads []*DatastorePayload) error {
	bulk := e.esClient.Bulk()

	var typeConfig ElasticsearchTypeConfig
	var indexString string

	for _, payload := range payloads {
		typeConfig = e.config.Types[payload.TypeName]

		indexString = typeConfig.Prefix + payload.Timestamp.Format(typeConfig.DateFormat)
		bulk.Add(elastic.NewBulkIndexRequest().Index(indexString).Type(typeConfig.MappingType).Doc(payload.Data))
	}

	ctx := context.Background()
	_, err := bulk.Do(ctx)
	return err
}

func (e *ElasticsearchDatastore) syncIndexTemplates() {
	// // Sync all the type settings we have
	// for typeName, typ := range c.State.Types {
	// 	err := typ.SyncIndexTemplate(client, c.State.Config)
	// 	if err != nil {
	// 		log.Printf("  ERROR: failed to sync index template for type %v", typeName)
	// 	}
	// }
	// log.Printf("  successfully synced type index templates")
	// if t.Config.Template == nil {
	// 	return nil
	// }
	//
	// templateConfig := t.Config.Template
	//
	// settings := make(map[string]interface{})
	// if templateConfig.NumReplicas != nil {
	// 	settings["number_of_replicas"] = templateConfig.NumReplicas
	// }
	//
	// if templateConfig.NumShards != nil {
	// 	settings["number_of_shards"] = templateConfig.NumShards
	// }
	//
	// if templateConfig.RefreshInterval != nil {
	// 	settings["refresh_interval"] = templateConfig.RefreshInterval
	// }
	//
	// payload := make(map[string]interface{})
	// payload["template"] = t.Config.Prefix + "*"
	// payload["settings"] = settings
	//
	// mappings := make(map[string]interface{})
	// for _, mappingName := range templateConfig.Mappings {
	// 	mappings[mappingName] = config.Mappings[mappingName].GenerateJSON()
	// }
	// payload["mappings"] = mappings
	//
	// templateService := elastic.NewIndicesPutTemplateService(esClient)
	// ctx := context.Background()
	// _, err := templateService.BodyJson(payload).Name(t.Config.Prefix + "template").Do(ctx)
	// return err
}

func (e *ElasticsearchDatastore) syncMappings() {
	for mappingName, mappingConfig := range e.config.Mappings {
		e.syncMapping(mappingName, mappingConfig)
	}
}

func (e *ElasticsearchDatastore) syncMapping(name string, config ElasticsearchMappingConfig) {
	result := make(map[string]interface{})

	properties := make(map[string]interface{})
	for k, v := range config.Fields {
		properties[k] = map[string]interface{}{
			"type":  v.Type,
			"index": v.Index,
		}
	}

	result["properties"] = properties

	ctx := context.Background()
	_, err := e.esClient.PutMapping().Type(name).BodyJson(result).Do(ctx)
	if err != nil {
		log.Printf("ERROR: failed to sync mapping %s: %v", name, err)
	}
}
