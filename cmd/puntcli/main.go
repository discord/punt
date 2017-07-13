package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

var (
	hosts      = flag.String("hosts", "localhost:1234", "comma deliminated list of hosts")
	streamType = flag.String("stream-type", "*", "steam type to follow")
	filter     = flag.String("filter", "", "JSON filter to apply")
)

func handleHost(host string, channel chan []byte) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		fmt.Printf("Error (%s): %s\n", host, err)
		return
	}

	writer := bufio.NewWriter(conn)
	writer.WriteString(fmt.Sprintf("tail %s %s\r\n", *streamType, *filter))
	writer.Flush()

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadBytes('\n')

		if err != nil {
			fmt.Printf("Error (%s): %s\n", host, err)
			break
		}

		channel <- line
	}
}

func main() {
	flag.Parse()

	channel := make(chan []byte, 0)

	for _, host := range strings.Split(*hosts, ",") {
		go handleHost(host, channel)
	}

	var line []byte
	for {
		line = <-channel
		os.Stdout.Write(line)
	}
}
