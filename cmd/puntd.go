package main

import (
	"../punt"
	"flag"
	"fmt"
	"log"
)

const (
	VERSION = "0.1.5"
)

var (
	version    = flag.Bool("version", false, "print version information and exit")
	configPath = flag.String("config", "config.json", "path to json configuration file")
)

func main() {
	flag.Parse()

	// Print the help message (and exit) if requested
	if *version {
		fmt.Printf("puntd v%v\n", VERSION)
		return
	}

	config, err := punt.LoadConfig(*configPath)

	if err != nil {
		log.Panicf("Failed to parse configuration: %v", err)
	}

	state := punt.NewState(config)
	state.Run()
	<-state.Exit
}
