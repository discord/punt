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

type preparedQuery struct {
	query  string
	fields []string
}

type ClickhouseConfig struct {
	URL             string
	Types           map[string]ClickhouseTypeConfig
	Batcher         DatastoreBatcherConfig
	PartitionFormat string
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

func (c *ClickhouseDatastore) Prune(typeName string, keep int) error {
	typeConfig, exists := c.config.Types[typeName]
	if !exists {
		return nil
	}

	// Get a list of all parts for this table
	rows, err := c.db.Query(`
		SELECT partition FROM system.parts WHERE table=? GROUP BY partition ORDER BY partition DESC
	`, typeConfig.Table)
	if err != nil {
		return err
	}

	// Figure out if we should delete any of these
	var (
		partition string
	)

	partitions := []string{}
	for rows.Next() {
		err := rows.Scan(&partition)
		if err != nil {
			return err
		}

		partitions = append(partitions, partition)
	}

	toDelete := partitions[:len(partitions)-keep]
	for _, partition = range toDelete {
		_, err = c.db.Exec(fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s", typeConfig.Table, partition))
		if err != nil {
			return err
		}
	}

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
			tx.Rollback()
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
		preparedQuery.fields = []string{"time"}
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

	for idx, fieldName := range preparedQuery.fields[1:] {
		columns[idx+1] = payload.ReadDataPath(typeConfig.Fields[fieldName])
	}

	return columns, nil
}
