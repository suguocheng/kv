package server

// import (
// 	"bufio"
// 	"fmt"
// 	"kv/kvstore"
// 	"net"
// 	"strings"
// )

// type Follower struct {
// 	KV *kvstore.KV
// }

// func (f *Follower) Start(addr string) error {
// 	listener, err := net.Listen("tcp", addr)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Println("Follower listening on", addr)

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			continue
// 		}

// 		go f.handleConnection(conn)
// 	}
// }

// func (f *Follower) handleConnection(conn net.Conn) {
// 	defer conn.Close()
// 	reader := bufio.NewReader(conn)
// 	for {
// 		line, err := reader.ReadString('\n')
// 		if err != nil {
// 			return
// 		}

// 		parts := strings.SplitN(strings.TrimSpace(line), " ", 3)
// 		if len(parts) < 2 {
// 			continue
// 		}

// 		switch parts[0] {
// 		case "PUT":
// 			if len(parts) == 3 {
// 				f.KV.Put(parts[1], parts[2])
// 			}
// 		case "DEL":
// 			f.KV.Delete(parts[1])
// 		}
// 	}
// }
