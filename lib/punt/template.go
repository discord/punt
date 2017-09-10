package punt

import (
	"context"
	"gopkg.in/olivere/elastic.v5"
)

type Template struct {
	Name     string
	Mappings []string `json:"mappings"`
}

func (t Template) GenerateJSON(config *Config) map[string]interface{} {
	result := make(map[string]interface{})

	result["template"] = t.Name

	mappings := make(map[string]interface{})
	for _, mappingName := range t.Mappings {
		mappings[mappingName] = config.Mappings[mappingName].GenerateJSON()
	}

	result["mappings"] = mappings
	return result
}

func (t Template) PutTemplate(client *elastic.Client, config *Config) error {
	ctx := context.Background()
	_, err := client.IndexPutTemplate(t.Name).BodyJson(t.GenerateJSON(config)).Do(ctx)
	if err != nil {
		return err
	}
	return nil
}
