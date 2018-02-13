package punt

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/kshvakov/clickhouse"
	"github.com/mitchellh/mapstructure"
)

var NoConnectionError = errors.New("No clickhouse connection available")
var InvalidSourceType = errors.New("Invalid source type")
var InvalidDestType = errors.New("Invalid destination type")

type ClickhouseConfig struct {
	URL   string
	Types map[string]ClickhouseTypeConfig
	DatastoreBatcherConfig
}

type ClickhouseTypeConfig struct {
	Table  string
	Fields map[string]ClickhouseFieldConfig
}

type ClickhouseFieldConfig struct {
	Source string
	Type   string
}

type ClickhouseDatastore struct {
	config            *ClickhouseConfig
	batcher           *DatastoreBatcher
	db                *sql.DB
	typeInsertQueries map[string]string
}

func NewClickhouseDatastore(config map[string]interface{}) *ClickhouseDatastore {
	clickhouse := &ClickhouseDatastore{
		config:            &ClickhouseConfig{},
		db:                nil,
		typeInsertQueries: make(map[string]string),
	}

	mapstructure.Decode(config, clickhouse.config)

	return clickhouse
}

func (c *ClickhouseDatastore) Initialize() error {
	c.batcher = NewDatastoreBatcher(c, &c.config.DatastoreBatcherConfig)
	c.connect()
	log.Printf("    connection to clickhouse opened")
	c.prepareQueries()
	log.Printf("    prepared all clickhouse queries")
	return nil
}

func (c *ClickhouseDatastore) Write(payload *DatastorePayload) error {
	return c.batcher.Write(payload)
}

func (c *ClickhouseDatastore) Flush() error {
	return c.batcher.Flush()
}

func (c *ClickhouseDatastore) Prune(typeName string, keepDuration time.Duration) error {
	return nil
}

func (c *ClickhouseDatastore) Commit(payloads []*DatastorePayload) error {
	if c.db == nil {
		c.connect()
		if c.db == nil {
			log.Printf("[CH] WARNING: failed to commit, no active database connection")
			return NoConnectionError
		}
	}

	// Map of table to transaction
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	var exists bool
	var query *sql.Stmt
	queries := make(map[string]*sql.Stmt)

	for _, payload := range payloads {
		if _, exists := c.typeInsertQueries[payload.TypeName]; !exists {
			continue
		}

		if _, exists = queries[payload.TypeName]; !exists {
			q, err := tx.Prepare(c.typeInsertQueries[payload.TypeName])
			if err != nil {
				return err
			}
			queries[payload.TypeName] = q
		}

		query = queries[payload.TypeName]
		row, err := c.prepare(payload)
		if err != nil {
			return err
		}

		_, err = query.Exec(row...)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (c *ClickhouseDatastore) connect() {
	db, err := sql.Open("clickhouse", c.config.URL)
	if err != nil {
		log.Printf("[CH] WARNING: failed to connect to clickhouse: %v", err)
		return
	}

	if err := db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	c.db = db
}

func (c *ClickhouseDatastore) prepareQueries() {
	for typeName, typeConfig := range c.config.Types {
		// Create an array of our field names
		fields := []string{"date", "time"}
		for fieldName, _ := range typeConfig.Fields {
			fields = append(fields, fieldName)
		}

		// Create an array of question marks
		argumentFillers := make([]string, len(fields))
		for idx, _ := range argumentFillers {
			argumentFillers[idx] = "?"
		}

		c.typeInsertQueries[typeName] = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			typeConfig.Table,
			strings.Join(fields, ", "),
			strings.Join(argumentFillers, ", "),
		)
	}
}

// Takes a payload and returns an array of columns
func (c *ClickhouseDatastore) prepare(payload *DatastorePayload) ([]interface{}, error) {
	typeConfig, exists := c.config.Types[payload.TypeName]
	if !exists {
		return nil, nil
	}

	columns := make([]interface{}, len(typeConfig.Fields)+2)
	columns[0] = payload.Timestamp
	columns[1] = payload.Timestamp

	var err error
	var converted interface{}

	idx := 0
	for _, fieldConfig := range typeConfig.Fields {
		columns[idx+2] = payload.ReadDataPath(fieldConfig.Source)

		if fieldConfig.Type != "" {
			converted, err = ConvertType(columns[idx+2], fieldConfig.Type)
			if err != nil {
				return nil, err
			}

			columns[idx+2] = converted
		}

		idx++
	}

	return columns, nil
}

func ConvertType(value interface{}, destType string) (interface{}, error) {
	switch value.(type) {
	case int:
		return convertTypeFromInt(value.(int), destType)
	case uint:
		return convertTypeFromUInt(value.(uint), destType)
	case float32:
		return convertTypeFromFloat(float64(value.(float32)), destType)
	case float64:
		return convertTypeFromFloat(value.(float64), destType)
	default:
		return nil, InvalidSourceType
	}
}

func convertTypeFromInt(value int, destType string) (interface{}, error) {
	switch destType {
	case "int":
		return value, nil
	case "uint":
		return uint(value), nil
	case "float32":
		return float32(value), nil
	case "float64":
		return float64(value), nil
	case "string":
		return string(value), nil
	default:
		return nil, InvalidDestType
	}
}

func convertTypeFromFloat(value float64, destType string) (interface{}, error) {
	switch destType {
	case "int":
		return int(value), nil
	case "uint":
		return uint(value), nil
	case "float32":
		return value, nil
	case "float64":
		return value, nil
	case "string":
		return fmt.Sprintf("%v", value), nil
	default:
		return nil, InvalidDestType
	}
}

func convertTypeFromUInt(value uint, destType string) (interface{}, error) {
	switch destType {
	case "int":
		return int(value), nil
	case "uint":
		return value, nil
	case "float32":
		return float32(value), nil
	case "float64":
		return float64(value), nil
	case "string":
		return string(value), nil
	default:
		return nil, InvalidDestType
	}
}
