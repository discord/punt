package punt

import (
	"context"
	"log"

	"github.com/olivere/elastic"
)

type TypeConfig struct {
	Prefix      string `json:"prefix"`
	MappingType string `json:"mapping_type"`
	DateFormat  string `json:"date_format"`
	Transformer struct {
		Name   string                 `json:"name"`
		Config map[string]interface{} `json:"config"`
	} `json:"transformer"`
	Mutators []map[string]interface{} `json:"mutators"`
	Template *struct {
		NumReplicas     *int     `json:"num_replicas"`
		NumShards       *int     `json:"num_shards"`
		RefreshInterval *string  `json:"refresh_interval"`
		Mappings        []string `json:"mappings"`
	} `json:"template"`
}

type TypeSubscriber struct {
	channel chan (map[string]interface{})
}

func NewTypeSubscriber() *TypeSubscriber {
	return &TypeSubscriber{
		channel: make(chan map[string]interface{}, 0),
	}
}

type Type struct {
	Config      TypeConfig
	Transformer Transformer
	Mutators    []Mutator
	Alerts      []*Alert
	subscribers []*TypeSubscriber
}

func NewType(config TypeConfig) *Type {
	mutators := make([]Mutator, 0)

	for _, mutator := range config.Mutators {
		inst, err := GetMutator(mutator["name"].(string), mutator["config"].(map[string]interface{}))
		if err != nil {
			log.Panicf("Failed to load mutator %v", mutator)
		}
		mutators = append(mutators, inst)
	}

	return &Type{
		Config:      config,
		Transformer: GetTransformer(config.Transformer.Name, config.Transformer.Config),
		Mutators:    mutators,
	}
}

func (t *Type) SyncIndexTemplate(esClient *elastic.Client, config *Config) error {
	if t.Config.Template == nil {
		return nil
	}

	templateConfig := t.Config.Template

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
	payload["template"] = t.Config.Prefix + "*"
	payload["settings"] = settings

	mappings := make(map[string]interface{})
	for _, mappingName := range templateConfig.Mappings {
		mappings[mappingName] = config.Mappings[mappingName].GenerateJSON()
	}
	payload["mappings"] = mappings

	templateService := elastic.NewIndicesPutTemplateService(esClient)
	ctx := context.Background()
	_, err := templateService.BodyJson(payload).Name(t.Config.Prefix + "template").Do(ctx)
	return err
}
