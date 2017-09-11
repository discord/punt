package punt

import (
	"github.com/oschwald/geoip2-golang"
	"net"
)

type Mutator interface {
	Mutate(map[string]interface{}) error
}

func GetMutator(name string, config map[string]interface{}) (Mutator, error) {
	switch name {
	case "geoip":
		return NewGeoIPMutator(config)
	default:
		return nil, nil
	}
}

type GeoIPMutator struct {
	InputField   string
	OutputFields map[string]interface{}

	reader *geoip2.Reader
}

func NewGeoIPMutator(config map[string]interface{}) (*GeoIPMutator, error) {
	mutator := &GeoIPMutator{}

	reader, err := geoip2.Open(config["path"].(string))
	if err != nil {
		return nil, err
	}
	mutator.reader = reader

	mutator.InputField = config["input_field"].(string)
	mutator.OutputFields = config["output_fields"].(map[string]interface{})

	return mutator, nil
}

func (m *GeoIPMutator) Mutate(data map[string]interface{}) error {
	input_field, exists := data[m.InputField]
	if !exists {
		return nil
	}

	ip := net.ParseIP(input_field.(string))

	city, err := m.reader.City(ip)
	if err != nil {
		return err
	}

	var output string
	for source, rawOutput := range m.OutputFields {
		output = rawOutput.(string)
		switch source {
		case "location_metro_code":
			data[output] = city.Location.MetroCode
		case "location_country_code":
			data[output] = city.Country.IsoCode
		case "location_geo":
			data[output] = []float64{
				city.Location.Latitude,
				city.Location.Longitude,
			}
		default:
		}
	}

	return nil
}
