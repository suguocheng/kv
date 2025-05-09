package server

import (
	"fmt"
	"net"
)

var followers []string

func SetFollowers(addrs []string) {
	followers = addrs
}

func Replicate(op, key, value string) {
	for _, addr := range followers {
		go func(addr string) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				fmt.Println("Failed to connect to follower:", err)
				return
			}
			defer conn.Close()

			var cmd string
			if op == "PUT" {
				cmd = fmt.Sprintf("PUT %s %s\n", key, value)
			} else if op == "DEL" {
				cmd = fmt.Sprintf("DEL %s\n", key)
			}
			conn.Write([]byte(cmd))
		}(addr)
	}
}
