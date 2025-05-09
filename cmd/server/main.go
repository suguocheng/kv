package main

import (
	"flag"
	"fmt"
	"kv/kvstore"
	"kv/server"
	"net"
	"strings"
)

func main() {
	followers := flag.String("followers", "", "comma-separated list of follower addresses")
	flag.Parse()

	kv, err := kvstore.NewKV("store.log")
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	// 设置 follower 地址
	if *followers != "" {
		server.SetFollowers(strings.Split(*followers, ","))
	}

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
		go server.HandleConnection(conn, kv)
	}
}
