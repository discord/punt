package punt

import (
	"context"
	"log"
	"sort"

	"gopkg.in/olivere/elastic.v5"
)

type GCConfig struct {
	Keep int `json:"keep"`
}

func GCIndexes(esClient *elastic.Client, prefix string, config GCConfig) {
	ctx := context.Background()
	indexes, err := elastic.NewIndicesGetService(esClient).Index(prefix + "*").Do(ctx)

	if err != nil {
		log.Printf("[GC] ERROR: failed to get list of indexes; %v", err)
		return
	}

	if len(indexes) <= config.Keep {
		return
	}

	sortedIndexNames := make([]string, 0)
	for key, _ := range indexes {
		sortedIndexNames = append(sortedIndexNames, key)
	}
	sort.Strings(sortedIndexNames)

	toDelete := sortedIndexNames[:config.Keep]

	log.Printf("[GC] Deleting the following indexes for `%v`: %v", prefix, toDelete)
	_, err = elastic.NewIndicesDeleteService(esClient).Index(toDelete).Do(ctx)
	if err != nil {
		log.Printf("[GC] ERROR: failed to delete indexes (%v): %v", toDelete, err)
	}
}
