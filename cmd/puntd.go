package main

import (
	"../punt"
	"flag"
)

var configPath = flag.String("config", "config.json", "path to json configuration file")

func main() {
	flag.Parse()
	config, err := punt.LoadConfig(*configPath)
	if err != nil {
		panic(err)
	}

	state := punt.NewState(config)
	state.Run()
	<-state.Exit
}
