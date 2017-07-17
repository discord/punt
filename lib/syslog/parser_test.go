package syslog

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSimple(t *testing.T) {
	msg, err := ParseRFC3164(
		"<190>Feb 22 04:23:32 test-hostname-with-stuff-1-32 my-application: this is a test oh boy!",
	)

	assert.Equal(t, err, nil)
	assert.Equal(t, 190, msg.Priority)
	assert.Equal(t, "test-hostname-with-stuff-1-32", msg.Hostname)
	assert.Equal(t, "my-application", msg.Tag)
	assert.Equal(t, "this is a test oh boy!", msg.Content)

	msg, err = ParseRFC3164(
		"<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick\non /dev/pts/8",
	)

	assert.Equal(t, nil, err)
	assert.Equal(t, 34, msg.Priority)
	assert.Equal(t, "'su root' failed for lonvick\non /dev/pts/8", msg.Content)
}
