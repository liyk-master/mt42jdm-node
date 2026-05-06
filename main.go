package main

import (
	"github.com/liyk-master/mt42jdm-node/app/do"
	"github.com/liyk-master/mt42jdm-node/app/lib"
)

func main() {
	cfg := lib.GetConfig()
	// 使用WebSocket连接获取数据
	do.DoWebsocket(cfg)
	// 保留原有的MySQL连接逻辑，如果需要可以取消注释
	// do.Do(cfg)
}
