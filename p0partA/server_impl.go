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
}

type keyValueServer struct {
	store                 kvstore.KVStore
	clients               map[net.Conn]*client
	closeChan             chan struct{}
	joinChan              chan net.Conn
	leaveChan             chan net.Conn
	droppedClientsCounter int32
	commandChannel        chan command // Channel for centralized store access to be thread-safe
}

type command struct {
	action   string
	key      string
	value    []byte
	oldValue []byte
	newValue []byte
	respChan chan string
}

func New(store kvstore.KVStore) KeyValueServer {
	kvs := &keyValueServer{
		store:          store,
		clients:        make(map[net.Conn]*client),
		closeChan:      make(chan struct{}),
		joinChan:       make(chan net.Conn),
		leaveChan:      make(chan net.Conn),
		commandChannel: make(chan command),
	}
	go kvs.storeManager()
	return kvs
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
	// I have used buffered channel to prevent Blocking for slow-reading clients
	kvs.clients[conn] = &client{
		writeChan: make(chan string, 500),
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
			kvs.handlePut(parts[1], []byte(parts[2]))
		case "Get":
			if len(parts) < 2 {
				continue
			}
			response := kvs.handleGet(parts[1])

			// Check if the Buffer Channel is full or not
			select {
			case cli.writeChan <- response:
			default:
				fmt.Println("Dropping message for slow-reading client")
			}

		case "Delete":
			if len(parts) < 2 {
				continue
			}
			kvs.handleDelete(parts[1])
		case "Update":
			if len(parts) < 4 {
				continue
			}
			kvs.handleUpdate(parts[1], []byte(parts[2]), []byte(parts[3]))
		}
	}
	kvs.leaveChan <- conn
}

func (kvs *keyValueServer) writeRoutine(conn net.Conn, cli *client) {
	for msg := range cli.writeChan {
		conn.Write([]byte(msg))
	}
}

func (kvs *keyValueServer) storeManager() {
	for cmd := range kvs.commandChannel {
		switch cmd.action {
		case "Put":
			kvs.store.Put(cmd.key, cmd.value)
			cmd.respChan <- "OK"
		case "Get":
			values := kvs.store.Get(cmd.key)
			response := ""
			for _, v := range values {
				response += fmt.Sprintf("%s:%s\n", cmd.key, string(v))
			}
			cmd.respChan <- response
		case "Delete":
			kvs.store.Delete(cmd.key)
			cmd.respChan <- "OK"
		case "Update":
			kvs.store.Update(cmd.key, cmd.oldValue, cmd.newValue)
			cmd.respChan <- "OK"
		}
	}
}

func (kvs *keyValueServer) handlePut(key string, value []byte) string {
	respChan := make(chan string)
	kvs.commandChannel <- command{
		action:   "Put",
		key:      key,
		value:    value,
		respChan: respChan,
	}
	return <-respChan
}

func (kvs *keyValueServer) handleGet(key string) string {
	respChan := make(chan string)
	kvs.commandChannel <- command{
		action:   "Get",
		key:      key,
		respChan: respChan,
	}
	return <-respChan
}

func (kvs *keyValueServer) handleDelete(key string) string {
	respChan := make(chan string)
	kvs.commandChannel <- command{
		action:   "Delete",
		key:      key,
		respChan: respChan,
	}
	return <-respChan
}

func (kvs *keyValueServer) handleUpdate(key string, oldValue, newValue []byte) string {
	respChan := make(chan string)
	kvs.commandChannel <- command{
		action:   "Update",
		key:      key,
		oldValue: oldValue,
		newValue: newValue,
		respChan: respChan,
	}
	return <-respChan
}
