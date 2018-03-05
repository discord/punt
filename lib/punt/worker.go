package punt

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/discordapp/punt/lib/punt/datastore"
)

var InvalidResultType = errors.New("InvalidResultType")
var InvalidSourceType = errors.New("InvalidSourceType")

type ClusterWorker struct {
	Cluster *Cluster

	datastores     []datastore.Datastore
	datastoreTypes map[string][]datastore.Datastore
	ctrl           chan string
	pruneLock      chan bool
}

func NewClusterWorker(cluster *Cluster) *ClusterWorker {
	datastoreTypes := make(map[string][]datastore.Datastore)
	datastores := make([]datastore.Datastore, 0)

	for datastoreName, datastoreConfigRaw := range cluster.Config.Datastores {
		log.Printf("  intializing datastore %s", datastoreName)
		datastoreType := datastoreConfigRaw.(map[string]interface{})["type"].(string)
		datastoreConfig := datastoreConfigRaw.(map[string]interface{})["config"].(map[string]interface{})
		ds := datastore.CreateDatastore(datastoreType, datastoreConfig)
		ds.Initialize(datastoreName, cluster.metrics)

		datastores = append(datastores, ds)

		subscribedTypes := ds.GetSubscribedTypes()
		if subscribedTypes == nil {
			continue
		}

		for _, typeName := range subscribedTypes {
			if _, exists := datastoreTypes[typeName]; !exists {
				datastoreTypes[typeName] = make([]datastore.Datastore, 0)
			}

			datastoreTypes[typeName] = append(datastoreTypes[typeName], ds)
		}
	}

	return &ClusterWorker{
		Cluster:        cluster,
		datastores:     datastores,
		datastoreTypes: datastoreTypes,
		ctrl:           make(chan string, 0),
		pruneLock:      make(chan bool, 1),
	}
}

func (cw *ClusterWorker) run() {
	flushInterval := cw.Cluster.Config.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 60
	}
	ticker := time.NewTicker(time.Duration(flushInterval) * time.Second)

	var startProcessing time.Time
	var err error
	var payload map[string]interface{}

	for {
		select {
		case msg := <-cw.Cluster.messages:
			// Increment the received metric
			tag := msg["tag"].(string)
			statsTags := []string{fmt.Sprintf("tag:%s", tag)}
			cw.Cluster.metrics.Incr("msgs.received", statsTags, 1)

			// Attempt to select the type based on the syslog tag
			typ := cw.Cluster.State.Types[tag]

			if typ == nil {
				tag = "*"
				typ = cw.Cluster.State.Types["*"]

				if typ == nil {
					log.Printf("Warning, recieved unhandled tag %v", tag)
					cw.Cluster.metrics.Incr("msgs.unhandled", statsTags, 1)
					continue
				}
			}

			// Grab this in case our transformer modifies it
			timestamp := msg["timestamp"].(time.Time)

			// Start a time as we process and mutate the message
			startProcessing = time.Now()

			// Transform the message using the type properties
			payload, err = typ.Transformer.Transform(msg)
			if err != nil {
				log.Printf("Failed to transform message `%v`: %v", msg, err)
				cw.Cluster.metrics.Incr("msgs.failed", statsTags, 1)
				continue
			}

			// Apply field type-casts
			for fieldName, fieldType := range typ.Config.FieldTypes {
				value, exists := datastore.ReadDataPath(payload, fieldName)
				if !exists {
					continue
				}

				value, err = typeCast(value, fieldType)
				if err != nil {
					datastore.WriteDataPath(payload, fieldName, nil)

					if cw.Cluster.Config.Debug {
						log.Printf("WARNING: failed to convert field %v: %v", fieldName, err)
					}
					continue
				}

				datastore.WriteDataPath(payload, fieldName, value)
			}

			// Apply all the mutators
			for _, mutator := range typ.Mutators {
				mutator.Mutate(payload)
			}

			payload["@timestamp"] = timestamp.Format("2006-01-02T15:04:05+00:00")
			payload["punt-server"] = cw.Cluster.hostname

			// Report the processing time
			cw.Cluster.metrics.Timing("processing_latency", time.Now().Sub(startProcessing), statsTags, 1)

			if cw.Cluster.Config.Debug {
				log.Printf("(%v) %v", tag, payload)
			}

			// Create a datastore payload and submit it to all our downstream datastores.
			datastorePayload := &datastore.DatastorePayload{
				TypeName:  tag,
				Timestamp: timestamp,
				Data:      payload,
			}
			for _, ds := range cw.datastoreTypes[tag] {
				ds.Write(datastorePayload)
			}

			// Distribute the message to all subscribers, using non-blocking send
			for _, sub := range typ.subscribers {
				select {
				case sub.channel <- payload:
				default:
				}
			}

			for _, alert := range typ.Alerts {
				alert.Run(payload)
			}

			cw.Cluster.metrics.Incr("msgs.processed", statsTags, 1)
		case ctrlMsg := <-cw.ctrl:
			if ctrlMsg == "exit" {
				return
			} else if ctrlMsg == "prune" {
				select {
				case cw.pruneLock <- true:
					go cw.prune()
				default:
					return
				}
			}
		case <-ticker.C:
			for _, ds := range cw.datastores {
				ds.Flush()
			}
		}
	}
}

func (cw *ClusterWorker) prune() {
	for typeName, typeConfig := range cw.Cluster.State.Types {
		if typeConfig.Config.PruneKeep == nil {
			continue
		}

		var err error
		for _, ds := range cw.datastoreTypes[typeName] {
			err = ds.Prune(typeName, *typeConfig.Config.PruneKeep)
			if err != nil {
				log.Printf("ERROR: failed to prune type %v on datastore %v: %v", typeName, ds, err)
				log.Printf("pruned %s", typeName)
			}
		}
	}

	<-cw.pruneLock
}

func typeCast(value interface{}, resultType string) (interface{}, error) {
	switch value.(type) {
	case float32:
		return typeCastFloat(float64(value.(float32)), resultType)
	case float64:
		return typeCastFloat(value.(float64), resultType)
	}
	return nil, InvalidSourceType
}

func typeCastFloat(value float64, resultType string) (interface{}, error) {
	switch resultType {
	case "int":
		return int(value), nil
	case "uint":
		return uint(value), nil
	case "string":
		return fmt.Sprintf("%f", value), nil
	}
	return nil, InvalidResultType
}
