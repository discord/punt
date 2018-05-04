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

	valid, validExists := payload.ReadDataPath("a.b.c")
	assert.Equal(t, 3, valid)
	assert.Equal(t, true, validExists)

	invalid, invalidExists := payload.ReadDataPath("a.b.d")
	assert.Equal(t, nil, invalid)
	assert.Equal(t, false, invalidExists)

	bad, badExists := payload.ReadDataPath("a.b.d.e.f.g")
	assert.Equal(t, nil, bad)
	assert.Equal(t, false, badExists)
}

func TestWriteDataPath(t *testing.T) {
	data := map[string]interface{}{
		"x": map[string]interface{}{
			"y": map[string]interface{}{
				"a": 1,
				"b": "2",
			},
		},
	}

	assert.Equal(t, true, WriteDataPath(data, "x.y.a", 2))
	assert.Equal(t, true, WriteDataPath(data, "x.y.b", "3"))
	assert.Equal(t, 2, data["x"].(map[string]interface{})["y"].(map[string]interface{})["a"])
	assert.Equal(t, "3", data["x"].(map[string]interface{})["y"].(map[string]interface{})["b"])
}
