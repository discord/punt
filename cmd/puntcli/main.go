package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

var (
	hosts      = flag.String("hosts", "localhost:1234", "comma deliminated list of hosts")
	streamType = flag.String("stream-type", "*", "steam type to follow")
)

func handleHost(host, streamType string) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	writer := bufio.NewWriter(conn)
	writer.WriteString(fmt.Sprintf("tail %s\r\n", streamType))
	writer.Flush()

	io.Copy(os.Stdout, conn)
}

func main() {
	flag.Parse()

	for _, host := range strings.Split(*hosts, ",") {
		go handleHost(host, *streamType)
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
