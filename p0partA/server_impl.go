package p0partA

import (
	"fmt"
	"net"
	"project0/p0partA/kvstore"
)

type client struct {
	writeChan chan string
	readChan  chan string
	isClosed  bool
}

type keyValueServer struct {
	store     kvstore.KVStore
	clients   map[net.Conn]*client
	closeChan chan struct{}
	joinChan  chan net.Conn
	leaveChan chan net.Conn
}

func New(store kvstore.KVStore) KeyValueServer {
	return &keyValueServer{
		store:     store,
		clients:   make(map[net.Conn]*client),
		closeChan: make(chan struct{}),
		joinChan:  make(chan net.Conn),
		leaveChan: make(chan net.Conn),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-kvs.closeChan:
					return
				default:
					continue
				}
			}
			kvs.joinChan <- conn
		}
	}()

	return nil
}

func (kvs *keyValueServer) Close() {
	close(kvs.closeChan)
	for conn := range kvs.clients {
		err := conn.Close()
		if err != nil {
			return
		}
	}
	kvs.clients = nil
}

func (kvs *keyValueServer) CountActive() int {
	counter := 0
	for _, client := range kvs.clients {
		if client.isClosed == false {
			counter++
		}
	}
	return counter
}

func (kvs *keyValueServer) CountDropped() int {
	counter := 0
	for _, client := range kvs.clients {
		if client.isClosed == true {
			counter++
		}
	}
	return counter
}
