package server

// import (
// 	"bufio"
// 	"fmt"
// 	"kv/kvstore"
// 	"net"
// 	"strings"
// )

// var followers []string

// func SetFollowers(addrs []string) {
// 	followers = addrs
// }

// func Replicate(op, key, value string) {
// 	for _, addr := range followers {
// 		go func(addr string) {
// 			conn, err := net.Dial("tcp", addr)
// 			if err != nil {
// 				fmt.Println("Failed to connect to follower:", err)
// 				return
// 			}
// 			defer conn.Close()

// 			var cmd string
// 			if op == "PUT" {
// 				cmd = fmt.Sprintf("PUT %s %s\n", key, value)
// 			} else if op == "DEL" {
// 				cmd = fmt.Sprintf("DEL %s\n", key)
// 			}
// 			conn.Write([]byte(cmd))
// 		}(addr)
// 	}
// }

// // HandleConnection 处理一个客户端连接
// func HandleConnection(conn net.Conn, kv *kvstore.KV) {
// 	defer conn.Close()
// 	reader := bufio.NewReader(conn)

// 	for {
// 		line, err := reader.ReadString('\n')
// 		if err != nil {
// 			fmt.Println("Connection closed:", err)
// 			return
// 		}

// 		line = strings.TrimSpace(line)
// 		parts := strings.SplitN(line, " ", 3)

// 		if len(parts) == 0 {
// 			conn.Write([]byte("ERR: empty command\n"))
// 			continue
// 		}

// 		var response string

// 		switch parts[0] {
// 		case "PUT":
// 			if len(parts) != 3 {
// 				response = "ERR: usage PUT key value\n"
// 			} else {
// 				ok := kv.Put(parts[1], parts[2])
// 				if ok == nil {
// 					Replicate("PUT", parts[1], parts[2])
// 					response = "OK\n"
// 				} else {
// 					response = "key already exists\n"
// 				}
// 			}
// 		case "GET":
// 			if len(parts) != 2 {
// 				response = "ERR: usage GET key\n"
// 			} else {
// 				val, ok := kv.Get(parts[1])
// 				if ok == nil {
// 					response = val + "\n"
// 				} else {
// 					response = "key does not exist\n"
// 				}
// 			}
// 		case "DEL":
// 			if len(parts) != 2 {
// 				response = "ERR: usage DEL key\n"
// 			} else {
// 				ok := kv.Delete(parts[1])
// 				if ok == nil {
// 					Replicate("DEL", parts[1], "")
// 					response = "OK\n"
// 				} else {
// 					response = "key does not exist\n"
// 				}
// 			}
// 		default:
// 			response = "ERR: unknown command\n"
// 		}

// 		conn.Write([]byte(response))
// 	}
// }
