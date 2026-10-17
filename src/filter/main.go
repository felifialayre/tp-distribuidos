package main

import (
	"os"
	"tp-distribuidos/filter/filter"
)

func run() int {
	worker, err := filter.NewFilter("input_queue", "rabbitmq", 5672)
	if err != nil {
		return 1
	}
	err = worker.Run()
	if err != nil {
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
