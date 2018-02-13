package punt

import "log"

type State struct {
	Config *Config

	Clusters map[string]*Cluster
	Types    map[string]*Type
	Alerts   map[string]*Alert
	Actions  map[string]*Action
	Exit     chan bool
}

func NewState(config *Config) *State {
	state := State{
		Config:   config,
		Clusters: make(map[string]*Cluster),
		Types:    make(map[string]*Type),
		Alerts:   make(map[string]*Alert),
		Actions:  make(map[string]*Action),
		Exit:     make(chan bool),
	}

	for name, cluster := range config.Clusters {
		state.Clusters[name] = NewCluster(&state, name, cluster)
	}

	for name, typeConfig := range config.Types {
		state.Types[name] = NewType(typeConfig)
	}

	for name, actionConfig := range config.Actions {
		state.Actions[name] = NewAction(&state, name, actionConfig)
	}

	for name, alertConfig := range config.Alerts {
		state.Alerts[name] = NewAlert(&state, name, alertConfig)

		// If no sources are provided, we assume all sources are wanted
		if len(state.Alerts[name].Config.Sources) == 0 {
			for _, typ := range state.Types {
				typ.Alerts = append(typ.Alerts, state.Alerts[name])
			}
		} else {
			for _, source := range state.Alerts[name].Config.Sources {
				state.Types[source].Alerts = append(state.Types[source].Alerts, state.Alerts[name])
			}
		}
	}

	return &state
}

func (s *State) Run() {
	log.Printf("Attempting to start Punt")

	for _, cluster := range s.Clusters {
		cluster.Run()
	}

	if s.Config.ControlSocket.Enabled {
		cs, err := NewControlSocket(s, s.Config.ControlSocket.Bind)
		if err != nil {
			log.Printf("Failed to create control socket: %v", err)
		} else {
			go cs.Run()
		}
	}

	log.Printf("Punt started")
}
