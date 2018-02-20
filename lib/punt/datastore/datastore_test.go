package datastore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDatastorePayloadReadDataPath(t *testing.T) {
	payload := &DatastorePayload{
		TypeName:  "test",
		Timestamp: time.Now(),
		Data: map[string]interface{}{
			"a": map[string]interface{}{
				"b": map[string]interface{}{
					"c": 3,
				},
			},
		},
	}

	assert.Equal(t, 3, payload.ReadDataPath("a.b.c"))
	assert.Equal(t, nil, payload.ReadDataPath("a.b.d"))
}
