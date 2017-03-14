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
	MODE_DELIMITER = iota
	MODE_OCTET_COUNTED
)

var (
	OctetRe = regexp.MustCompile(`^(\d+)`)

	InvalidOctetErr = errors.New("Invalid Octet Prefix")
)

type SyslogData map[string]interface{}

type InvalidMessage struct {
	Data  string
	Error error
}

type Server struct {
	Mode     int
	Messages chan SyslogData
	Errors   chan InvalidMessage
}

type ConnState struct {
	Addr   string
	Buffer []byte
	Size   int
	OctLen int
}

type udpReadFunc func([]byte) (int, net.Addr, error)

func NewServer(mode int, messages chan SyslogData, errors chan InvalidMessage) *Server {
	return &Server{
		Mode:     mode,
		Messages: messages,
		Errors:   errors,
	}
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

		state := ConnState{
			Addr:   client.RemoteAddr().String(),
			Buffer: make([]byte, 0),
			Size:   0,
		}

		buf := make([]byte, 64000)

		for {
			sz, err = client.Read(buf)

			if sz == 0 || err != nil {
				break
			}

			state.Size += sz
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

	state := ConnState{
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

func (s *Server) handlePayloadSafe(msg *SyslogMessage, state *ConnState) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from handlePayload: %v / `%v`", r, state.Buffer)
		}
	}()

	s.handlePayload(msg, state)
}

func (s *Server) handlePayload(msg *SyslogMessage, state *ConnState) {
	var lines []string

	// New line delimited
	if s.Mode == MODE_DELIMITER {
		lines = strings.Split(string(state.Buffer[:state.Size]), "\n")
	} else if s.Mode == MODE_OCTET_COUNTED {
		for {
			if len(state.Buffer) == 0 {
				break
			}

			// If this isn't a continuation on a previous payload, read the octet length
			if state.OctLen == 0 {
				// Find an octet count
				match := OctetRe.Find(state.Buffer)
				if match == nil {
					s.Errors <- InvalidMessage{Data: string(state.Buffer), Error: InvalidOctetErr}
					return
				}

				// Convert to an integer
				size, err := strconv.Atoi(string(match))
				if err != nil {
					s.Errors <- InvalidMessage{Data: string(state.Buffer), Error: err}
					return
				}

				state.OctLen = size
			}

			if len(state.Buffer) >= state.OctLen {
				offset := len(strconv.FormatInt(int64(state.OctLen), 10)) + 1
				lines = append(lines, string(state.Buffer[offset:offset+state.OctLen]))

				if len(state.Buffer)-state.OctLen > 0 {
					state.Buffer = state.Buffer[offset+state.OctLen:]
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
		select {
		case s.Errors <- InvalidMessage{Data: data, Error: err}:
			return
		default:
			return
		}
	}

	parts := msg.ToMapping()
	parts["source"] = addr
	s.Messages <- parts
}
