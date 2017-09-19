package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/discordapp/punt/lib/punt"
)

var (
	hosts      = flag.String("hosts", "localhost:1234", "comma deliminated list of hosts")
	streamType = flag.String("stream-type", "*", "steam type to follow")
	filter     = flag.String("filter", "", "JSON filter to apply")
	sample     = flag.Int("sample", 100.00, "Percentage of log lines to randomly sample")
)

func handleHost(host string, channel chan []byte) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		fmt.Printf("Error (%s): %s\n", host, err)
		return
	}

	if *sample < 0 || *sample > 100 {
		fmt.Printf("Error, sample must be a number between 1 and 100 (%v)\n", *sample)
		return
	}

	// Parse the filter
	parsedFilter := make(map[string]string)
	// parsedFilter := map[string]
	if len(*filter) > 0 {
		err := json.Unmarshal([]byte(*filter), &parsedFilter)
		if err != nil {
			fmt.Printf("Error, invalid filter: %v\n", err)
			return
		}

	}

	request := punt.TailRequest{
		StreamType: *streamType,
		Filter:     parsedFilter,
		Sample:     *sample,
	}

	data, err := json.Marshal(&request)
	if err != nil {
		fmt.Printf("Error, failed to marshal request json: %v\n", err)
		return
	}

	writer := bufio.NewWriter(conn)
	writer.WriteString(fmt.Sprintf("tail %s\r\n", string(data)))
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
