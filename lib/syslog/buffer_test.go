package syslog

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIncompleteBufferRead(t *testing.T) {
	data := []byte("5")

	buffer := NewSyslogBuffer()
	buffer.Append(data)

	assert.Equal(t, 0, buffer.scanSize())
	assert.Equal(t, []byte(nil), buffer.Next())
	assert.Equal(t, "5", string(buffer.Buffer))

	buffer.Append([]byte(" test "))

	assert.Equal(t, "5 test ", string(buffer.Buffer))
	assert.Equal(t, "test ", string(buffer.Next()))
}

func TestBufferScanSize(t *testing.T) {
	data := []byte("5 test 4 wtf 3 lol 2   ")

	buffer := NewSyslogBuffer()
	buffer.Append(data)

	assert.Equal(t, 5, buffer.scanSize())
	assert.Equal(t, "test 4 wtf 3 lol 2   ", string(buffer.Buffer))
}

func TestBufferScan(t *testing.T) {
	data := []byte("5 test 4 wtf 3 lol 2   ")

	buffer := NewSyslogBuffer()
	buffer.Append(data)

	result := buffer.Next()
	assert.NotEqual(t, nil, result)
	assert.Equal(t, "test ", string(result))
	assert.Equal(t, "4 wtf 3 lol 2   ", string(buffer.Buffer))

	result = buffer.Next()
	assert.NotEqual(t, nil, result)
	assert.Equal(t, "wtf ", string(result))
}

func TestRsyslogScan(t *testing.T) {
	data := []byte("98 <133>Mar 14 04:20:29 example-host-prod-1-1 audit type=SYSCALL msg=audit(1489465219.995:1699): test")

	buffer := NewSyslogBuffer()
	buffer.Append(data)

	result := buffer.Next()
	assert.NotEqual(t, nil, result)
	assert.Equal(t, "<133>Mar 14 04:20:29 example-host-prod-1-1 audit type=SYSCALL msg=audit(1489465219.995:1699): test", string(result))
}

func TestInvalidData(t *testing.T) {
	data := []byte("asdfasdfasdf 10 abcdefghij")

	buffer := NewSyslogBuffer()
	buffer.Append(data)

	result := buffer.Next()
	assert.NotEqual(t, nil, result)
	assert.Equal(t, "abcdefghij", string(result))
}

func TestNewlineData(t *testing.T) {
	data := []byte("a\nb\ncdef\n")

	buffer := NewSyslogBuffer()
	buffer.Append(data)

	result := buffer.NextLine()
	assert.NotEqual(t, nil, result)
	assert.Equal(t, "a", string(result))
	assert.Equal(t, "b\ncdef\n", string(buffer.Buffer))

	result = buffer.NextLine()
	assert.NotEqual(t, nil, result)
	assert.Equal(t, "b", string(result))

	result = buffer.NextLine()
	assert.NotEqual(t, nil, result)
	assert.Equal(t, "cdef", string(result))
}
