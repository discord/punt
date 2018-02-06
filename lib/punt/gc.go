package punt

import (
	"context"
	"log"
	"sort"

	"github.com/olivere/elastic"
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

	// Sort the array, this will end up being older -> newer
	sortedIndexNames := make([]string, 0)
	for key, _ := range indexes {
		sortedIndexNames = append(sortedIndexNames, key)
	}
	sort.Strings(sortedIndexNames)

	// Now that we've sorted indexes, we want to select all but the latest config.Keep
	//  indexes and delete those.
	toDelete := sortedIndexNames[:len(sortedIndexNames)-config.Keep]

	var toDeleteBuff []string
	for len(toDelete) > 0 {
		if len(toDelete) > 25 {
			toDeleteBuff = toDelete[:25]
			toDelete = toDelete[25:]
		} else {
			toDeleteBuff = toDelete
			toDelete = make([]string, 0)
		}

		log.Printf("[GC] Deleting the following indexes for `%v`: %v", prefix, toDeleteBuff)
		_, err = elastic.NewIndicesDeleteService(esClient).Index(toDeleteBuff).Do(ctx)
		if err != nil {
			log.Printf("[GC] ERROR: failed to delete indexes (%v): %v", toDeleteBuff, err)
		}
	}
}
