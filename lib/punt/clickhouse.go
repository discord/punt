package punt

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/kshvakov/clickhouse"
	"github.com/mitchellh/mapstructure"
)

var NoConnectionError = errors.New("No clickhouse connection available")

type ClickhouseConfig struct {
	URL       string
	Types     map[string]ClickhouseTypeConfig
	BatchSize int
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
	c.batcher = NewDatastoreBatcher(c, c.config.BatchSize)
	c.connect()
	c.prepareQueries()
	return nil
}

func (c *ClickhouseDatastore) Write(payload *DatastorePayload) error {
	return c.batcher.Write(payload)
}

func (c *ClickhouseDatastore) Flush() error {
	return c.batcher.Flush()
}

func (c *ClickhouseDatastore) Commit(payloads []*DatastorePayload) error {
	if c.db == nil {
		c.connect()
		if c.db == nil {
			// TODO(az): retry / backoff logic? Should go in batch?
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

	log.Printf("[CH] Connected OK")
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

	idx := 0
	for _, fieldConfig := range typeConfig.Fields {
		// TODO(az): type conversion
		columns[idx+2] = payload.ReadDataPath(fieldConfig.Source)
	}

	return columns, nil
}
