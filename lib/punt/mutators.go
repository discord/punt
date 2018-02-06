package punt

import (
	"net"
	"strconv"
	"time"

	"github.com/oschwald/geoip2-golang"
)

type Mutator interface {
	Mutate(map[string]interface{}) error
}

func GetMutator(name string, config map[string]interface{}) (Mutator, error) {
	switch name {
	case "geoip":
		return NewGeoIPMutator(config)
	case "unixtime":
		return NewUnixTimeMutator(config)
	default:
		return nil, nil
	}
}

type UnixTimeMutator struct {
	Fields map[string]string
	Format string
}

func NewUnixTimeMutator(config map[string]interface{}) (*UnixTimeMutator, error) {
	mutator := &UnixTimeMutator{Format: "2006-01-02T15:04:05+00:00"}
	mutator.Fields = config["fields"].(map[string]string)

	if format, exists := config["format"]; exists {
		mutator.Format = format.(string)
	}

	return mutator, nil
}

func (m *UnixTimeMutator) Mutate(data map[string]interface{}) error {
	for inputField, outputField := range m.Fields {
		inputFieldData, exists := data[inputField]
		if !exists {
			return nil
		}

		value, err := strconv.ParseFloat(inputFieldData.(string), 64)
		if err != nil {
			return err
		}

		data[outputField] = time.Unix(int64(value), int64(value*1000000000)).Format(m.Format)
	}

	return nil
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
	inputFieldData, exists := data[m.InputField]
	if !exists {
		return nil
	}

	ip := net.ParseIP(inputFieldData.(string))

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
				city.Location.Longitude,
				city.Location.Latitude,
			}
		default:
		}
	}

	return nil
}
