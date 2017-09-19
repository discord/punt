package punt

import (
	"encoding/json"

	"github.com/discordapp/punt/lib/syslog"
)

type Transformer interface {
	Transform(parts syslog.SyslogData) (map[string]interface{}, error)
}

func GetTransformer(name string, config map[string]interface{}) Transformer {
	switch name {
	case "direct":
		return NewDirectTransformer(config)
	case "unpack-merge":
		return NewUnpackMergeTransformer(config)
	case "unpack-take":
		return NewUnpackTakeTransformer(config)
	default:
		return nil
	}
}

// Doesn't perform any transformation or parsing on the syslog structure
type DirectTransformer struct{}

func NewDirectTransformer(config map[string]interface{}) *DirectTransformer {
	return &DirectTransformer{}
}

func (b *DirectTransformer) Transform(parts syslog.SyslogData) (map[string]interface{}, error) {
	return parts, nil
}

// Parses the log line as JSON and merges it into the syslog structure
type UnpackMergeTransformer struct{}

func NewUnpackMergeTransformer(config map[string]interface{}) *UnpackMergeTransformer {
	return &UnpackMergeTransformer{}
}

func (u *UnpackMergeTransformer) Transform(parts syslog.SyslogData) (map[string]interface{}, error) {
	err := json.Unmarshal([]byte(parts["content"].(string)), &parts)
	delete(parts, "content")
	return parts, err
}

// Parses the log line as JSON and uses it as the core structure (ignoring syslog data)
type UnpackTakeTransformer struct{}

func NewUnpackTakeTransformer(config map[string]interface{}) *UnpackTakeTransformer {
	return &UnpackTakeTransformer{}
}

func (u *UnpackTakeTransformer) Transform(parts syslog.SyslogData) (map[string]interface{}, error) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(parts["content"].(string)), &data)
	return data, err
}
