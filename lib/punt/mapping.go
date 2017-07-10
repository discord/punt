package punt

import (
	"context"
	"gopkg.in/olivere/elastic.v5"
)

type MappingField struct {
	Type  string `json:"type"`
	Index bool   `json:"index"`
}

type Mapping struct {
	Name   string                  `json:"name"`
	Fields map[string]MappingField `json:"fields"`
	All    bool                    `json:"all"`
}

func (m Mapping) GenerateJSON() map[string]interface{} {
	result := make(map[string]interface{})

	properties := make(map[string]interface{})
	for k, v := range m.Fields {
		properties[k] = map[string]interface{}{
			"type":  v.Type,
			"index": v.Index,
		}
	}

	result["properties"] = properties
	return result
}

func (m Mapping) PutMapping(client *elastic.Client) error {
	final := m.GenerateJSON()

	ctx := context.Background()
	_, err := client.PutMapping().Type(m.Name).BodyJson(final).Do(ctx)
	if err != nil {
		return err
	}
	return nil
}
