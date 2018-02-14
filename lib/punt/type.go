package punt

import (
	"log"
	"time"
)

type TypeConfig struct {
	Transformer struct {
		Name   string                 `json:"name"`
		Config map[string]interface{} `json:"config"`
	} `json:"transformer"`
	Mutators []map[string]interface{} `json:"mutators"`
	Prune    *struct {
		Hours int `json:"hours"`
	} `json"prune"`
	FieldTypes map[string]string `json:"field_types"`
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

	pruneKeepDuration time.Duration
	subscribers       []*TypeSubscriber
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

	var pruneKeepDuration time.Duration
	if config.Prune != nil {
		pruneKeepDuration += time.Duration(config.Prune.Hours) * time.Hour
	}

	return &Type{
		Config:            config,
		Transformer:       GetTransformer(config.Transformer.Name, config.Transformer.Config),
		Mutators:          mutators,
		pruneKeepDuration: pruneKeepDuration,
	}
}
