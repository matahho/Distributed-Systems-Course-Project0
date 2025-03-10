// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"fmt"
	"net"
	"project0/p0partA/kvstore"
	"strings"
	"sync/atomic"
)

type keyValueServer struct {
	store          kvstore.KVStore
	activeClients  int32
	droppedClients int32
	clients        map[net.Conn]chan string
	closeCh        chan struct{}
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	server := &keyValueServer{
		store:   store,
		clients: make(map[net.Conn]chan string),
		closeCh: make(chan struct{}),
	}
	return server
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
				case <-kvs.closeCh:
					return
				default:
					continue
				}
			}
			kvs.clients[conn] = make(chan string, 500)
			atomic.AddInt32(&kvs.activeClients, 1)
			go kvs.handleClient(conn)
		}
	}()

	return nil
}

func (kvs *keyValueServer) Close() {
	close(kvs.closeCh)
	for conn, ch := range kvs.clients {
		close(ch)
		conn.Close()
	}
	kvs.clients = nil
}

func (kvs *keyValueServer) CountActive() int {
	return int(kvs.activeClients)
}

func (kvs *keyValueServer) CountDropped() int {
	return int(kvs.droppedClients)
}

// TODO: add additional methods/functions below!

func (kvs *keyValueServer) handleClient(conn net.Conn) {
	defer func() {
		delete(kvs.clients, conn)
		atomic.AddInt32(&kvs.activeClients, -1)
		atomic.AddInt32(&kvs.droppedClients, 1)
		conn.Close()
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) < 2 {
			continue
		}

		switch parts[0] {
		case "Put":
			if len(parts) == 3 {
				kvs.store.Put(parts[1], []byte(parts[2]))
			}
		case "Get":
			values := kvs.store.Get(parts[1])
			for _, v := range values {
				conn.Write([]byte(fmt.Sprintf("%s:%s\n", parts[1], string(v))))
			}
		case "Delete":
			kvs.store.Delete(parts[1])
		case "Update":
			if len(parts) == 4 {
				kvs.store.Update(parts[1], []byte(parts[2]), []byte(parts[3]))
			}
		}
	}
}
