package punt

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type ClusterWorker struct {
	Cluster *Cluster

	datastores []Datastore
	name       string
	lock       *sync.RWMutex
	ctrl       chan string
}

func NewClusterWorker(cluster *Cluster) *ClusterWorker {
	datastores := make([]Datastore, 0)

	for datastoreName, datastore := range cluster.Config.Datastores {
		log.Printf("  intializing datastore %s", datastoreName)
		datastoreType := datastore.(map[string]interface{})["type"].(string)
		datastoreConfig := datastore.(map[string]interface{})["config"].(map[string]interface{})
		datastore := CreateDatastore(datastoreType, datastoreConfig)
		datastore.Initialize()
		datastores = append(datastores, datastore)
	}

	return &ClusterWorker{
		Cluster:    cluster,
		datastores: datastores,
		lock:       &sync.RWMutex{},
		ctrl:       make(chan string, 0),
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

			// Apply all the mutators
			for _, mutator := range typ.Mutators {
				mutator.Mutate(payload)
			}

			// indexString := typ.Config.Prefix + timestamp.Format(typ.Config.DateFormat)
			payload["@timestamp"] = timestamp.Format("2006-01-02T15:04:05+00:00")
			payload["punt-server"] = cw.Cluster.hostname

			// Report the processing time
			cw.Cluster.metrics.Timing("processing_latency", time.Now().Sub(startProcessing), statsTags, 1)

			if cw.Cluster.Config.Debug {
				log.Printf("(%v) %v", tag, payload)
			}

			// Create a datastore payload and submit it to all our downstream datastores.
			datastorePayload := &DatastorePayload{
				TypeName:  tag,
				Timestamp: timestamp,
				Data:      payload,
			}
			for _, datastore := range cw.datastores {
				datastore.Write(datastorePayload)
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
				for typeName, typeConfig := range cw.Cluster.State.Types {
					if typeConfig.pruneKeepDuration == time.Duration(0) {
						continue
					}

					for _, datastore := range cw.datastores {
						datastore.Prune(typeName, typeConfig.pruneKeepDuration)
					}
				}
			}
		case <-ticker.C:
			for _, datastore := range cw.datastores {
				datastore.Flush()
			}
		}
	}
}
