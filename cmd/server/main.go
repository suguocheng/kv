package main

func main() {
	me := parseNodeID()
	conf := loadNodeConfig(me)

	kv := initKV(conf.WALDir, conf.MaxWALEntries)
	defer kv.Close()

	rf, applyCh := initRaft(conf, kv)

	// 启动 Raft gRPC 服务端，监听 peer 通信端口
	startRaftGRPCServer(rf, conf.PeerAddrs[me])

	startApplyLoop(rf, kv, applyCh)

	startClientListener(conf.ClientAddr, kv, rf)
}
