package punt

import (
	"encoding/json"
	"io/ioutil"
)

type ControlSocketConfig struct {
	Bind    string `json:"bind"`
	Enabled bool   `json:"enabled"`
}

type Config struct {
	Clusters      map[string]ClusterConfig `json:"clusters"`
	Clickhouse    *ClickhouseConfig        `json:"clickhouse"`
	Types         map[string]TypeConfig    `json:"types"`
	Mappings      map[string]Mapping       `json:"mappings"`
	Alerts        map[string]AlertConfig   `json:"alerts"`
	Actions       map[string]ActionConfig  `json:"actions"`
	ControlSocket ControlSocketConfig      `json:"control_socket"`
	GC            map[string]GCConfig      `json:"gc"`
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
