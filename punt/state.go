package punt

import "log"

type State struct {
	Config *Config

	Clusters map[string]*Cluster
	Types    map[string]*Type
	Exit     chan bool
}

func NewState(config *Config) *State {
	state := State{
		Config:   config,
		Clusters: make(map[string]*Cluster),
		Types:    make(map[string]*Type),
		Exit:     make(chan bool),
	}

	for name, cluster := range config.Clusters {
		state.Clusters[name] = NewCluster(&state, name, cluster)
	}

	for name, typeConfig := range config.Types {
		state.Types[name] = NewType(typeConfig)
	}

	return &state
}

func (s *State) Run() {
	log.Printf("Attempting to start Punt")

	for _, cluster := range s.Clusters {
		cluster.Run()

		for _, mapping := range s.Config.Mappings {
			err := mapping.PutMapping(cluster.esClient)
			if err != nil {
				log.Printf("Failed to create mapping: %v", err)
			}
		}
	}
}
