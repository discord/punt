package punt

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/kshvakov/clickhouse"
)

var BufferFull error = errors.New("Clickhouse buffer is full, dropping message")

type ClickhouseConfig struct {
	URL        string                          `json:"url"`
	Types      map[string]ClickhouseTypeConfig `json:"types"`
	BufferSize int                             `json:"buffer_size"`
	BulkSize   int                             `json:"bulk_size"`
}

type ClickhouseTypeConfig struct {
	Table  string   `json:"table"`
	Fields []string `json:"fields"`
}

type ClickhousePayload struct {
	TypeName  string
	Timestamp time.Time
	Payload   map[string]interface{}
}

type Clickhouse struct {
	Config *ClickhouseConfig

	writeLock         sync.Mutex
	bulkBufferIdx     int
	bulkBuffer        []*ClickhousePayload
	writeBuffer       chan []*ClickhousePayload
	db                *sql.DB
	typeInsertQueries map[string]string
}

func NewClickhouse(config *ClickhouseConfig) *Clickhouse {
	clickhouse := &Clickhouse{
		Config:            config,
		bulkBufferIdx:     0,
		bulkBuffer:        make([]*ClickhousePayload, config.BulkSize),
		writeBuffer:       make(chan []*ClickhousePayload, config.BufferSize),
		typeInsertQueries: make(map[string]string),
	}
	go clickhouse.writeThread()
	go clickhouse.writeSweeper()
	clickhouse.initialize()
	clickhouse.connect()
	return clickhouse
}

func (c *Clickhouse) connect() {
	db, err := sql.Open("clickhouse", c.Config.URL)
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

func (c *Clickhouse) initialize() {
	for typeName, typeConfig := range c.Config.Types {
		qmarks := make([]string, len(typeConfig.Fields)+2)
		for idx := 0; idx < len(typeConfig.Fields)+2; idx++ {
			qmarks[idx] = "?"
		}

		fields := []string{"date", "time"}

		for _, field := range typeConfig.Fields {
			fields = append(fields, field)
		}

		c.typeInsertQueries[typeName] = fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s)",
			typeConfig.Table,
			strings.Join(fields, ", "),
			strings.Join(qmarks, ", "),
		)
	}
}

func (c *Clickhouse) Write(typeName string, timestamp time.Time, payload map[string]interface{}) error {
	if _, exists := c.typeInsertQueries[typeName]; !exists {
		return nil
	}

	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	c.bulkBuffer[c.bulkBufferIdx] = &ClickhousePayload{typeName, timestamp, payload}
	c.bulkBufferIdx++
	if c.bulkBufferIdx >= c.Config.BulkSize {
		select {
		case c.writeBuffer <- c.bulkBuffer:
			break
		default:
			return BufferFull
		}

		c.bulkBuffer = make([]*ClickhousePayload, cap(c.bulkBuffer))
		c.bulkBufferIdx = 0
	}

	return nil
}

func (c *Clickhouse) writeSweeper() {
	ticker := time.NewTicker(time.Second * 5)
	for range ticker.C {
		c.writeLock.Lock()
		c.writeBuffer <- c.bulkBuffer
		c.bulkBuffer = make([]*ClickhousePayload, cap(c.bulkBuffer))
		c.bulkBufferIdx = 0
		c.writeLock.Unlock()
	}
}

func (c *Clickhouse) writeThread() {
	var insertMap map[string][][]interface{}
	var err error
	var bulkPayload []*ClickhousePayload

	for {
		insertMap = make(map[string][][]interface{})
		bulkPayload = <-c.writeBuffer

		for _, payload := range bulkPayload {
			if payload == nil {
				break
			}

			if _, exists := insertMap[payload.TypeName]; !exists {
				insertMap[payload.TypeName] = make([][]interface{}, 0)
			}

			insertMap[payload.TypeName] = append(insertMap[payload.TypeName], c.preparePayload(payload))
		}

		for typeName, inserts := range insertMap {
			err = c.write(typeName, inserts)
			if err != nil {
				log.Printf("[CH] error writing bulk data: %v", err)
			}
		}
	}
}

func (c *Clickhouse) preparePayload(payload *ClickhousePayload) []interface{} {
	typeConfig := c.Config.Types[payload.TypeName]
	args := make([]interface{}, len(typeConfig.Fields)+2)

	args[0] = payload.Timestamp
	args[1] = args[0]
	for idx, fieldName := range typeConfig.Fields {
		args[idx+2] = payload.Payload[fieldName]
	}

	return args
}

func (c *Clickhouse) write(typeName string, inserts [][]interface{}) error {
	if c.db == nil {
		c.connect()
		if c.db == nil {
			return nil
		}
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		c.db = nil
		return err
	}

	query, err := tx.Prepare(c.typeInsertQueries[typeName])

	for _, insert := range inserts {
		_, err = query.Exec(insert...)
		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
