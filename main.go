package main

import (
	"fmt"
	"kv/kvstore"
	"kv/server"
	"net"
)

func main() {
	kv, err := kvstore.NewKV("store.log")
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	listener, err := net.Listen("tcp", ":8888")
	if err != nil {
		panic(err)
	}
	fmt.Println("KV server is running at :8888")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		fmt.Println("a client is connected")
		go server.HandleConnection(conn, kv)
	}
}
