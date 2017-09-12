package punt

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
)

type ControlSocket struct {
	bind     string
	listener net.Listener
	state    *State
}

type TailRequest struct {
	StreamType string
	Filter     map[string]string
	Sample     int
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
			log.Printf("[CS] Closed")
			break
		}

		parts := strings.SplitN(string(line), " ", 2)

		switch parts[0] {
		case "tail":
			tailData := TailRequest{}
			err := json.Unmarshal([]byte(parts[1]), &tailData)
			if err != nil {
				log.Printf("[CS] Received malformed tail request payload")
				continue
			}
			cs.handleCommandTail(&tailData, reader, writer)
		}
	}
}

func (cs *ControlSocket) handleCommandTail(data *TailRequest, reader *bufio.Reader, writer *bufio.Writer) {
	if _, exists := cs.state.Types[data.StreamType]; !exists {
		writer.WriteString(fmt.Sprintf("Unknown type '%s'\n", data.StreamType))
		writer.Flush()
		return
	}

	log.Printf("[CS] Tail starting on %s", data.StreamType)

	sub := NewTypeSubscriber()
	typ := cs.state.Types[data.StreamType]
	typ.subscribers = append(typ.subscribers, sub)
	defer func() {
		for _, value := range typ.subscribers {
			if value != sub {
				typ.subscribers = append(typ.subscribers, value)
			}
		}
	}()

	var matched bool
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

		// Filter the messages based on our information
		if len(data.Filter) > 0 {
			matched = true
			for k, v := range data.Filter {
				if data, exists := msg[k]; !exists || data.(string) != v {
					matched = false
					break
				}
			}

			if !matched {
				continue
			}
		}

		// If we're sampling the data, do that now
		if data.Sample != 100 {
			if rand.Intn(100) > data.Sample {
				continue
			}
		}

		writer.Write(bytes)
		writer.WriteString("\n")
		err = writer.Flush()
		if err != nil {
			log.Printf("[CS] Tail closed on %s", data.StreamType)
			return
		}
	}
}
