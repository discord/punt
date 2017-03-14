package syslog

import (
	"errors"
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

		addr := client.RemoteAddr().String()
		buf := make([]byte, 4096)

		for {
			sz, err = client.Read(buf)

			if sz == 0 || err != nil {
				break
			}

			s.handlePayload(&msg, string(buf[:sz]), addr)
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

	buf := make([]byte, 32000)

	for {

		sz, addr, err = read(buf)

		if err != nil || sz <= 0 {
			break
		}

		s.handlePayload(&msg, string(buf[:sz]), addr.String())
	}
}

func (s *Server) handlePayload(msg *SyslogMessage, buf string, addr string) {
	var lines []string

	// New line delimited
	if s.Mode == MODE_DELIMITER {
		lines = strings.Split(buf, "\n")
	} else if s.Mode == MODE_OCTET_COUNTED {
		for {
			if len(buf) == 0 {
				break
			}

			// Find an octet count
			match := OctetRe.Find([]byte(buf))
			if match == nil {
				s.Errors <- InvalidMessage{Data: buf, Error: InvalidOctetErr}
				return
			}

			// Convert to an integer
			size, err := strconv.Atoi(string(match))
			if err != nil {
				s.Errors <- InvalidMessage{Data: buf, Error: err}
				return
			}

			offset := len(match) + 1
			lines = append(lines, string(buf[offset:offset+size]))
			if len(buf)-size <= 0 {
				break
			}

			buf = buf[offset+size:]
		}
	}

	for _, line := range lines {
		if len(line) > 0 {
			s.parse(msg, line, addr)
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
