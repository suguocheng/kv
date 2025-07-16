package main

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"kv/pkg/proto/kvpb" // 替换为你的实际包路径
	"os"
)

func main() {
	data, err := os.ReadFile("data/node2/snapshot.pb")
	if err != nil {
		panic(err)
	}
	var snap kvpb.MVCCStore
	if err := proto.Unmarshal(data, &snap); err != nil {
		panic(err)
	}

	fmt.Printf("====== 快照内容输出 ======\n")
	fmt.Printf("当前修订号: %d\n", snap.GetCurrentRevision())
	fmt.Printf("键值对数量: %d\n", len(snap.GetPairs()))
	fmt.Println("详细内容:")
	for i, pair := range snap.GetPairs() {
		fmt.Printf("  [%d] key: %q\n", i+1, pair.GetKey())
		fmt.Printf("      value: %q\n", pair.GetValue())
		fmt.Printf("      ttl: %d\n", pair.GetTtl())
		fmt.Printf("      created_revision: %d\n", pair.GetCreatedRevision())
		fmt.Printf("      mod_revision: %d\n", pair.GetModRevision())
		fmt.Printf("      version: %d\n", pair.GetVersion())
		fmt.Printf("      deleted: %v\n", pair.GetDeleted())
	}
}
