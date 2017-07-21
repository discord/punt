package syslog

import (
	"strconv"
	"unicode"
)

type SyslogBuffer struct {
	Buffer   []byte
	Size     int
	LastSize int
}

func NewSyslogBuffer() *SyslogBuffer {
	return &SyslogBuffer{
		Buffer:   make([]byte, 0),
		Size:     0,
		LastSize: 0,
	}
}

func (sb *SyslogBuffer) Append(data []byte) {
	sb.Buffer = append(sb.Buffer, data...)
	sb.Size += len(data)
}

func (sb *SyslogBuffer) scanSize() int {
	var read int
	var buffer []byte
	var complete bool = false

	for _, char := range sb.Buffer {
		read += 1

		if unicode.IsDigit(int32(char)) {
			buffer = append(buffer, char)
			continue
		}

		if len(buffer) > 0 && char == ' ' {
			complete = true
			break
		}
	}

	if !complete {
		return 0
	}

	// TODO: if bad data gets in our buffer, we will never flush it

	value, err := strconv.Atoi(string(buffer))
	if err != nil {
		return 0
	}

	sb.Buffer = sb.Buffer[read:]
	sb.Size -= read
	return value
}

func (sb *SyslogBuffer) Next() []byte {
	if sb.LastSize == 0 {
		sb.LastSize = sb.scanSize()
		if sb.LastSize == 0 {
			return nil
		}
	}

	if sb.Size < sb.LastSize {
		return nil
	} else {
		data := sb.Buffer[:sb.LastSize]
		sb.Buffer = sb.Buffer[sb.LastSize:]
		sb.Size -= sb.LastSize
		sb.LastSize = 0
		return data
	}

	return nil
}

func (sb *SyslogBuffer) NextLine() []byte {
	var data []byte

	for _, char := range sb.Buffer {
		if char == '\n' {
			sb.Buffer = sb.Buffer[len(data)+1:]
			return data
		}

		data = append(data, char)
	}

	return nil
}
