package mqtt

import (
	"fmt"
	"net"
)

type Broker struct{}

func (b Broker) Serve(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}

		go b.handleConnection(c)
	}
}

func (b Broker) handleConnection(c net.Conn) {
	defer c.Close()
	fh, err := ReadFixedHeader(c)
	if err != nil {
		fmt.Println(err)
		return
	}
	if fh.Type != CONNECT {
		return
	}
	_, err = ReadConnectPacket(c, fh, true)
	if err != nil {
		fmt.Println(err)
		return
	}
}
