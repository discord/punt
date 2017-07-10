package syslog

import (
	"errors"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
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
	Buffer []byte
	Size   int
	OctLen int
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
			Buffer: make([]byte, 0),
			Size:   0,
		}

		buf := make([]byte, 4096)

		for {
			sz, err = client.Read(buf)

			if sz == 0 || err != nil {
				break
			}

			// state.Size += sz
			state.Buffer = append(state.Buffer, buf[:sz]...)

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
		Buffer: make([]byte, 32000),
	}

	for {
		sz, addr, err = read(state.Buffer)

		state.Size = sz
		state.Addr = addr.String()

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

	// New line delimited
	if s.config.TCPFrameMode == FRAME_MODE_DELIMITER {
		lines = strings.Split(string(state.Buffer[:state.Size]), "\n")
	} else if s.config.TCPFrameMode == FRAME_MODE_OCTET_COUNTED {
		for {
			if len(state.Buffer) == 0 {
				break
			}

			// If this isn't a continuation on a previous payload, read the octet length
			if state.OctLen == 0 {
				// Find an octet count
				match := OctetRe.Find(state.Buffer)
				if match == nil {
					s.sendInvalidMessage(InvalidMessage{Data: string(state.Buffer), Error: InvalidOctetErr})
					state.Buffer = []byte{}
					return
				}

				// Convert to an integer
				size, err := strconv.Atoi(string(match))
				if err != nil {
					state.Buffer = []byte{}
					s.sendInvalidMessage(InvalidMessage{Data: string(state.Buffer), Error: err})
					return
				}

				state.OctLen = size
			}

			if len(state.Buffer) >= state.OctLen {
				offset := len(strconv.FormatInt(int64(state.OctLen), 10)) + 1
				lines = append(lines, string(state.Buffer[offset:offset+state.OctLen]))

				if len(state.Buffer)-state.OctLen > 0 {
					state.Buffer = state.Buffer[offset+state.OctLen:]
				} else {
					state.Buffer = []byte{}
				}

				state.OctLen = 0
			} else {
				// Waiting for more
				break
			}
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
