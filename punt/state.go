package punt

import "log"

type State struct {
	Config *Config

	Clusters map[string]*Cluster
	Indexes  []*Index
	Exit     chan bool
}

func NewState(config *Config) *State {
	state := State{
		Config:   config,
		Clusters: make(map[string]*Cluster),
		Indexes:  make([]*Index, len(config.Indexes)),
		Exit:     make(chan bool),
	}

	for name, cluster := range config.Clusters {
		state.Clusters[name] = NewCluster(cluster)
	}

	for i, index := range config.Indexes {
		state.Indexes[i] = NewIndex(index, state.Clusters[index.Cluster])
	}

	return &state
}

func (s *State) Run() {
	for _, cluster := range s.Clusters {
		cluster.Run()

		for _, mapping := range s.Config.Mappings {
			err := mapping.PutMapping(cluster.esClient)
			if err != nil {
				log.Printf("Failed to create mapping: %v", err)
			}
		}
	}

	for _, index := range s.Indexes {
		go index.Run()
	}
}
