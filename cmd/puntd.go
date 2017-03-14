package main

import (
	"../punt"
	"flag"
	"log"
)

var configPath = flag.String("config", "config.json", "path to json configuration file")

func main() {
	flag.Parse()
	config, err := punt.LoadConfig(*configPath)

	if err != nil {
		log.Panicf("Failed to parse configuration: %v", err)
	}

	state := punt.NewState(config)
	state.Run()
	<-state.Exit
}
