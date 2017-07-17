package syslog

import (
	"errors"
	"log"
	"net"
	"regexp"
)

const (
	FRAME_MODE_DELIMITER = iota
	FRAME_MODE_OCTET_COUNTED

	FORMAT_RFC3164 = iota
)

var (
	OctetRe = regexp.MustCompile(`^(\d+)`)

	InvalidOctetErr = errors.New("Invalid Octet Prefix")
)

// A wrapper type for structured SyslogData
type SyslogData map[string]interface{}

// Struct which contains an invalid Syslog message that failed to parse. This
//  can be used to introspect and monitor errors.
type InvalidMessage struct {
	Data  string
	Error error
}

// Used to configure the Syslog Server
type ServerConfig struct {
	// The TCP framing mode to operate in (only valid for TCP)
	TCPFrameMode int

	// The syslog format to use
	Format int
}

type Server struct {
	Messages chan SyslogData
	Errors   chan InvalidMessage

	parser ParserInplaceFunc
	config *ServerConfig
}

type connState struct {
	Addr   string
	Buffer *SyslogBuffer
}

type udpReadFunc func([]byte) (int, net.Addr, error)

func NewServer(config *ServerConfig, messages chan SyslogData, errors chan InvalidMessage) *Server {
	server := Server{
		Messages: messages,
		Errors:   errors,
		config:   config,
	}

	if config.Format == FORMAT_RFC3164 {
		server.parser = ParseRFC3164Inplace
	} else {
		log.Panicf("Unknown syslog format: %v", config.Format)
	}

	return &server
}

func (s *Server) AddUDPListener(li net.Conn) {
	switch c := li.(type) {
	case *net.UDPConn:
		go s.handleUDP(func(buf []byte) (int, net.Addr, error) {
			return c.ReadFromUDP(buf)
		})
	case *net.UnixConn:
		go s.handleUDP(func(buf []byte) (int, net.Addr, error) {
			return c.ReadFromUnix(buf)
		})
	}
}

func (s *Server) AddTCPListener(li net.Listener) {
	go s.handleTCP(li)
}

func (s *Server) handleTCP(conn net.Listener) {
	handleTCPConn := func(client net.Conn) {
		var sz int
		var err error
		var msg SyslogMessage

		state := connState{
			Addr:   client.RemoteAddr().String(),
			Buffer: NewSyslogBuffer(),
		}

		buf := make([]byte, 4096)

		for {
			sz, err = client.Read(buf)

			if sz == 0 || err != nil {
				break
			}

			state.Buffer.Append(buf[:sz])
			s.handlePayloadSafe(&msg, &state)
		}
	}

	for {
		conn, err := conn.Accept()
		if err != nil {
			panic(err)
		}

		go handleTCPConn(conn)
	}
}

func (s *Server) handleUDP(read udpReadFunc) {
	var sz int
	var addr net.Addr
	var err error
	var msg SyslogMessage

	state := connState{
		Buffer: NewSyslogBuffer(),
	}

	buf := make([]byte, 4096)

	for {
		sz, addr, err = read(buf)

		state.Addr = addr.String()
		state.Buffer.Append(buf[:sz])

		if err != nil || sz <= 0 {
			break
		}

		s.handlePayloadSafe(&msg, &state)
	}
}

func (s *Server) handlePayloadSafe(msg *SyslogMessage, state *connState) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from handlePayload: %v / `%v`", r, state.Buffer)
		}
	}()

	s.handlePayload(msg, state)
}

func (s *Server) sendInvalidMessage(msg InvalidMessage) {
	select {
	case s.Errors <- msg:
		break
	default:
		break
	}
}

func (s *Server) handlePayload(msg *SyslogMessage, state *connState) {
	var lines []string
	var line []byte

	// New line delimited
	if s.config.TCPFrameMode == FRAME_MODE_DELIMITER {
		for {
			line = state.Buffer.NextLine()
			if line == nil {
				break
			}

			lines = append(lines, string(line))
		}
	} else if s.config.TCPFrameMode == FRAME_MODE_OCTET_COUNTED {
		for {
			line = state.Buffer.Next()
			if line == nil {
				break
			}

			lines = append(lines, string(line))
		}
	}

	for _, line := range lines {
		if len(line) > 0 {
			s.parse(msg, line, state.Addr)
		}
	}
}

func (s *Server) parse(msg *SyslogMessage, data string, addr string) {
	// Parse the syslog message
	err := ParseRFC3164Inplace(msg, data)

	// If we an encounter an error, attempt a non-blocking insert into the errors
	//  channel.
	if err != nil {
		s.sendInvalidMessage(InvalidMessage{Data: data, Error: err})
	}

	parts := msg.ToMapping()
	parts["source"] = addr
	s.Messages <- parts
}
