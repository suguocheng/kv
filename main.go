package main

import (
	"fmt"
	"kv/kvstore"
)

func main() {
	kv, err := kvstore.NewKV("store.log")
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	kv.Put("username", "alice")
	kv.Put("age", "23")

	val, _ := kv.Get("username")
	fmt.Println("username:", val)

	kv.Delete("age")
}
