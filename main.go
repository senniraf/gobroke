package main

import (
	"fmt"
	"net"
	"os"

	"github.com/senniraf/gobroke/mqtt"
)

func main() {
	address := ":1883"
	ln, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("Error creating server on %q: %v", address, err)
		os.Exit(-1)
	}

	err = mqtt.Broker{}.Serve(ln)
	if err != nil {
		fmt.Printf("Error accepting connection: %v", err)
		os.Exit(-1)
	}
}
