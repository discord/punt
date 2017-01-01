package main

import (
	"../punt"
	"flag"
	"fmt"
)

var configPath = flag.String("config", "config.json", "path to json configuration file")

func main() {
	flag.Parse()
	config, err := punt.LoadConfig(*configPath)
	if err != nil {
		panic(err)
	}

	// First, start all the clusters up one by one
	for name, cluster := range config.Clusters {
		fmt.Printf("Starting cluster %v\n", name)
		err := cluster.Run()
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("All clusters started, running\n")

	<-make(chan bool, 1)
}
