package main

func main() {
	me := parseNodeID()
	conf := loadNodeConfig(me)

	kv := initKV(conf.WALDir, conf.MaxWALEntries)
	defer kv.Close()

	rf, applyCh := initRaft(conf, kv)

	startApplyLoop(rf, kv, applyCh)

	startClientListener(conf.ClientAddr, kv, rf)
}
