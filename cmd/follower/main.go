package main

// import (
// 	"fmt"
// 	"kv/kvstore"
// 	"kv/server"
// 	"os"
// 	"strings"
// )

// func main() {
// 	addr := ":9001"
// 	if len(os.Args) > 1 {
// 		addr = os.Args[1]
// 	}

// 	port := strings.TrimPrefix(addr, ":")
// 	logPath := fmt.Sprintf("store-follower-%s.log", port)
// 	kv, _ := kvstore.NewKV(logPath)
// 	f := &server.Follower{KV: kv}

// 	if err := f.Start(addr); err != nil {
// 		panic(err)
// 	}
// }
