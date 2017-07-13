package punt

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
)

type ControlSocket struct {
	bind     string
	listener net.Listener
	state    *State
}

func NewControlSocket(state *State, bind string) (*ControlSocket, error) {
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}

	return &ControlSocket{
		bind:     bind,
		listener: listener,
		state:    state,
	}, nil
}

func (cs *ControlSocket) Run() {
	log.Printf("[CS] listening on %s", cs.bind)

	for {
		conn, err := cs.listener.Accept()
		if err != nil {
			log.Printf("[CS] Failed to accept new control socket connection: %s", err)
			continue
		}

		log.Printf("[CS] New connection opened")
		go cs.handleConnection(conn)
	}
}

func (cs *ControlSocket) handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		line, _, err := reader.ReadLine()

		if err != nil {
			break
		}

		parts := strings.SplitN(string(line), " ", 2)

		switch parts[0] {
		case "tail":
			cs.handleCommandTail(parts[1], reader, writer)
		}
	}
}

func (cs *ControlSocket) handleCommandTail(args string, reader *bufio.Reader, writer *bufio.Writer) {
	parts := strings.Split(string(args), " ")
	typeName := parts[0]

	if _, exists := cs.state.Types[typeName]; !exists {
		writer.WriteString(fmt.Sprintf("Unknown type '%s'\r\n", typeName))
		writer.Flush()
		return
	}

	log.Printf("[CS] Tail starting on %s", typeName)

	sub := NewTypeSubscriber()
	typ := cs.state.Types[typeName]
	typ.subscribers = append(typ.subscribers, sub)
	defer func() {
		for _, value := range typ.subscribers {
			if value != sub {
				typ.subscribers = append(typ.subscribers, value)
			}
		}
	}()

	var err error
	var bytes []byte
	var msg map[string]interface{}

	for {
		msg = <-sub.channel
		bytes, err = json.Marshal(msg)
		if err != nil {
			log.Printf("[CS] failed to marshal json")
			continue
		}

		writer.Write(bytes)
		writer.WriteString("\r\n")
		err = writer.Flush()
		if err != nil {
			return
		}
	}
}
