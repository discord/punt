package punt

type TypeConfig struct {
	Prefix      string `json:"prefix"`
	MappingType string `json:"mapping_type"`
	DateFormat  string `json:"date_format"`
	Transformer struct {
		Name   string                 `json:"name"`
		Config map[string]interface{} `json:"config"`
	} `json:"transformer"`
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
	Alerts      []*Alert
	subscribers []*TypeSubscriber
}

func NewType(config TypeConfig) *Type {
	return &Type{
		Config:      config,
		Transformer: GetTransformer(config.Transformer.Name, config.Transformer.Config),
	}
}
