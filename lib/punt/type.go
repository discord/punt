package punt

import (
	"log"
)

type TypeConfig struct {
	Transformer struct {
		Name   string                 `json:"name"`
		Config map[string]interface{} `json:"config"`
	} `json:"transformer"`
	Mutators   []map[string]interface{} `json:"mutators"`
	PruneKeep  *int                     `json"prune_keep"`
	FieldTypes map[string]string        `json:"field_types"`
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
