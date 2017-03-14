package syslog

import (
	"fmt"
	"testing"
)

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a == b {
		return
	}

	t.Fatal(fmt.Sprintf("%v != %v", a, b))
}

func TestSimple(t *testing.T) {
	msg, err := ParseRFC3164([]byte(
		"<190>Feb 22 04:23:32 discord-api-prd-1-11 discord-api: this is a test oh boy!",
	))

	assertEqual(t, err, nil)
	assertEqual(t, msg.Priority, 190)
	assertEqual(t, msg.Hostname, "discord-api-prd-1-11")
	assertEqual(t, msg.Tag, "discord-api")
	assertEqual(t, msg.Content, "this is a test oh boy!")
}
