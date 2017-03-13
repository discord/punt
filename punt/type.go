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

type Type struct {
	Config      TypeConfig
	Transformer Transformer
}

func NewType(config TypeConfig) *Type {
	return &Type{
		Config:      config,
		Transformer: GetTransformer(config.Transformer.Name, config.Transformer.Config),
	}
}
