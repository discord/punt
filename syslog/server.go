package syslog

import (
	"net"
)

type SyslogData map[string]interface{}

type InvalidMessage struct {
	Data  []byte
	Error error
}

type Server struct {
	Messages chan SyslogData
	Errors   chan InvalidMessage
}

type udpReadFunc func([]byte) (int, net.Addr, error)

func NewServer(messages chan SyslogData, errors chan InvalidMessage) *Server {
	return &Server{
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

			s.parse(&msg, buf[:sz], addr)
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

		s.parse(&msg, buf[:sz], addr.String())
	}
}

func (s *Server) parse(msg *SyslogMessage, data []byte, addr string) {
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
