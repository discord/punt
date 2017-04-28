package punt

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	Clusters map[string]ClusterConfig `json:"clusters"`
	Types    map[string]TypeConfig    `json:"types"`
	Mappings []Mapping                `json:"mappings"`
	Alerts   map[string]AlertConfig   `json:"alerts"`
	Actions  map[string]ActionConfig  `json:"actions"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := Config{}
	err = json.Unmarshal(file, &config)
	return &config, nil
}
