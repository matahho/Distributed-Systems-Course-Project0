package p0partA

import (
	"bufio"
	"fmt"
	"net"
	"project0/p0partA/kvstore"
	"strings"
)

type client struct {
	writeChan chan string
	readChan  chan string
}

type keyValueServer struct {
	store                 kvstore.KVStore
	clients               map[net.Conn]*client
	closeChan             chan struct{}
	joinChan              chan net.Conn
	leaveChan             chan net.Conn
	droppedClientsCounter int32
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
	go kvs.run()
	go func() {
		defer listener.Close()
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
		close(kvs.clients[conn].writeChan)
		err := conn.Close()
		if err != nil {
			return
		}
	}
	kvs.clients = nil
}

func (kvs *keyValueServer) CountActive() int {
	return len(kvs.clients)
}

func (kvs *keyValueServer) CountDropped() int {
	return int(kvs.droppedClientsCounter)
}

func (kvs *keyValueServer) run() {
	for {
		select {
		case <-kvs.closeChan:
			return
		case conn := <-kvs.joinChan:
			cli := kvs.newConnection(conn)
			go kvs.writeRoutine(conn, cli)
			go kvs.readRoutine(conn, cli)
		case conn := <-kvs.leaveChan:
			kvs.closeClient(conn)
		}
	}
}

func (kvs *keyValueServer) newConnection(conn net.Conn) *client {
	if kvs.clients[conn] != nil {
		return kvs.clients[conn]
	}
	// I have used buffered channel to prevent Blocking
	kvs.clients[conn] = &client{
		writeChan: make(chan string, 500),
		readChan:  make(chan string),
	}
	return kvs.clients[conn]
}

func (kvs *keyValueServer) closeClient(conn net.Conn) {
	client, ok := kvs.clients[conn]
	if ok {
		close(client.writeChan)
		err := conn.Close()
		if err != nil {
			return
		}
		delete(kvs.clients, conn)
	}
	//atomic.AddInt32(&kvs.droppedClientsCounter, 1)   We should not use atomic library
	kvs.droppedClientsCounter++
}

func (kvs *keyValueServer) readRoutine(conn net.Conn, cli *client) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) < 2 {
			continue
		}
		switch parts[0] {
		case "Put":
			if len(parts) < 3 {
				continue
			}
			key := parts[1]
			value := []byte(parts[2])
			kvs.store.Put(key, value)

		case "Get":
			if len(parts) < 2 {
				continue
			}
			key := parts[1]
			values := kvs.store.Get(key)
			for _, v := range values {
				cli.writeChan <- fmt.Sprintf("%s:%s\n", parts[1], string(v))
			}

		case "Delete":
			if len(parts) < 2 {
				continue
			}
			key := parts[1]
			kvs.store.Delete(key)

		case "Update":
			if len(parts) < 4 {
				continue
			}
			key := parts[1]
			oldValue := []byte(parts[2])
			newValue := []byte(parts[3])
			kvs.store.Update(key, oldValue, newValue)
		}
	}

	kvs.leaveChan <- conn
}

func (kvs *keyValueServer) writeRoutine(conn net.Conn, cli *client) {
	defer func() {
		kvs.leaveChan <- conn
	}()

	for {
		select {
		case <-kvs.closeChan:
			return
		case msg, ok := <-cli.writeChan:
			if !ok {
				return // Channel closed
			}
			_, err := conn.Write([]byte(msg))
			if err != nil {
				return // Client disconnected or error occurred
			}
		}
	}
}
