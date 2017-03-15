package syslog

import (
	"errors"
	"regexp"
	"strconv"
	"time"
)

var (
	RFC3164 = regexp.MustCompile(`<([0-9]+)>([A-Z][a-z][a-z]\s{1,2}\d{1,2}\s\d{2}[:]\d{2}[:]\d{2})\s([\w][\w\d\.@-]*)\s([^:[ ]+)[:[ ](.*)`)

	InvalidMessageError   = errors.New("Invalid Message")
	InvalidPriortyError   = errors.New("Invalid Priority")
	InvalidTimestampError = errors.New("Invalid Timestamp")

	timestampFormats = []string{
		"Jan 02 15:04:05",
		"Jan  2 15:04:05",
	}
)

type ParserInplaceFunc func(msg *SyslogMessage, data string) error

type SyslogMessage struct {
	Priority  int
	Timestamp time.Time
	Hostname  string
	Tag       string
	Content   string
}

// Returns a mapping
func (sm *SyslogMessage) ToMapping() map[string]interface{} {
	return map[string]interface{}{
		"priority":  sm.Priority,
		"timestamp": sm.Timestamp,
		"hostname":  sm.Hostname,
		"tag":       sm.Tag,
		"content":   sm.Content,
	}
}

func ParseRFC3164(data string) (*SyslogMessage, error) {
	msg := &SyslogMessage{}
	return msg, ParseRFC3164Inplace(msg, data)
}

// A parser which dumps results into an existing SyslogMessage struct. This can
//  be used for better performance and to avoid allocations
func ParseRFC3164Inplace(msg *SyslogMessage, data string) error {
	var err error

	found := RFC3164.FindAllStringSubmatch(data, -1)

	// If we don't have anything in the array, we didn't even match
	if len(found) == 0 {
		return InvalidMessageError
	}

	msg.Priority, err = strconv.Atoi(found[0][1])
	if err != nil {
		return InvalidPriortyError
	}

	for _, format := range timestampFormats {
		msg.Timestamp, err = time.Parse(format, found[0][2])

		if err == nil {
			if msg.Timestamp.Year() == 0 {
				msg.Timestamp = time.Date(
					time.Now().Year(),
					msg.Timestamp.Month(),
					msg.Timestamp.Day(),
					msg.Timestamp.Hour(),
					msg.Timestamp.Minute(),
					msg.Timestamp.Second(),
					msg.Timestamp.Nanosecond(),
					msg.Timestamp.Location(),
				)
			}

			break
		}
	}

	if err != nil {
		return InvalidTimestampError
	}

	msg.Hostname = found[0][3]
	msg.Tag = found[0][4]
	msg.Content = found[0][5]
	return nil
}
