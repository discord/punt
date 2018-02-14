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

// var InvalidSourceType = errors.New("Invalid source type")
// var InvalidDestType = errors.New("Invalid destination type")

type preparedQuery struct {
	query  string
	fields []string
}

type ClickhouseConfig struct {
	URL     string
	Types   map[string]ClickhouseTypeConfig
	Batcher DatastoreBatcherConfig
}

type ClickhouseTypeConfig struct {
	Table  string
	Fields map[string]string
}

type ClickhouseDatastore struct {
	config  *ClickhouseConfig
	batcher *DatastoreBatcher
	db      *sql.DB

	preparedQueries map[string]*preparedQuery
}

func NewClickhouseDatastore(config map[string]interface{}) *ClickhouseDatastore {
	clickhouse := &ClickhouseDatastore{
		config:          &ClickhouseConfig{},
		db:              nil,
		preparedQueries: make(map[string]*preparedQuery),
	}

	mapstructure.Decode(config, clickhouse.config)

	return clickhouse
}

func (c *ClickhouseDatastore) Initialize() error {
	c.batcher = NewDatastoreBatcher(c, &c.config.Batcher)
	c.connect()
	log.Printf("    connection to clickhouse opened")
	c.prepareQueries()
	log.Printf("    prepared all clickhouse queries")
	return nil
}

func (c *ClickhouseDatastore) GetSubscribedTypes() []string {
	if len(c.config.Types) == 0 {
		return nil
	}

	result := []string{}
	for typeName, _ := range c.config.Types {
		result = append(result, typeName)
	}
	return result
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
		preparedQuery := c.preparedQueries[payload.TypeName]
		if preparedQuery == nil {
			continue
		}

		if _, exists = queries[payload.TypeName]; !exists {
			q, err := tx.Prepare(preparedQuery.query)
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
		preparedQuery := &preparedQuery{}

		// Create an array of our field names
		preparedQuery.fields = []string{"date", "time"}
		for fieldName, _ := range typeConfig.Fields {
			preparedQuery.fields = append(preparedQuery.fields, fieldName)
		}

		// Create an array of question marks
		argumentFillers := make([]string, len(preparedQuery.fields))
		for idx, _ := range argumentFillers {
			argumentFillers[idx] = "?"
		}

		preparedQuery.query = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			typeConfig.Table,
			strings.Join(preparedQuery.fields, ", "),
			strings.Join(argumentFillers, ", "),
		)

		c.preparedQueries[typeName] = preparedQuery
	}
}

// Takes a payload and returns an array of columns
func (c *ClickhouseDatastore) prepare(payload *DatastorePayload) ([]interface{}, error) {
	typeConfig, exists := c.config.Types[payload.TypeName]
	if !exists {
		return nil, nil
	}

	preparedQuery := c.preparedQueries[payload.TypeName]
	if preparedQuery == nil {
		return nil, nil
	}

	columns := make([]interface{}, len(preparedQuery.fields))
	columns[0] = payload.Timestamp
	columns[1] = payload.Timestamp

	for idx, fieldName := range preparedQuery.fields[2:] {
		columns[idx+2] = payload.ReadDataPath(typeConfig.Fields[fieldName])
	}

	return columns, nil
}
