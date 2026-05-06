package do

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/liyk-master/mt42jdm-node/app/lib"
)

// 定义WebSocket数据结构
type MarketData struct {
	R string `json:"r"` // 回购价
	S string `json:"s"` // 销售价
	H string `json:"h"` // 最高价
	L string `json:"l"` // 最低价
	T string `json:"t"` // 时间
}

// 定义UDP发送的数据结构（与server.go中保持一致）
type WebsocketUdpdata struct {
	Code      string `json:"Code"`
	Volume    string `json:"Volume"`
	QuoteTime int64  `json:"QuoteTime"`
	Last      string `json:"Last"`
	Open      string `json:"Open"`
	High      string `json:"High"`
	Low       string `json:"Low"`
	LastClose string `json:"LastClose"`
	Buy       string `json:"Buy"`
	Sell      string `json:"Sell"`
}

var wsConfig *lib.Config

// DoWebsocket 启动WebSocket连接和数据处理
func DoWebsocket(cfg *lib.Config) {
	wsConfig = cfg

	// WebSocket URL
	// wsURL := "ws://ws3.9999j.cn/ws/market_all/jc.9999j.cn/15814600700?t=TU47O23W542F60b4Ad8ao9ql7Nq4JB5eg5bM76D1Kia2X"
	wsURL := wsConfig.WSURL

	// 创建中断信号通道，用于优雅退出
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	log.Println("正在连接到WebSocket服务器...")

	// 连接到WebSocket服务器
	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatal("连接失败:", err)
	}
	defer c.Close()

	log.Println("连接成功")

	// 创建一个通道用于接收消息
	done := make(chan struct{})

	// 存储最新的筛选数据
	// lastFilteredData := make(map[string]MarketData)

	// 启动一个goroutine来接收消息
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("读取消息错误:", err)
				return
			}

			// 处理接收到的消息
			// handleWsMessage(message, lastFilteredData)
			handleWsMessage(message)
		}
	}()

	// 每秒发送一次消息
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			err := c.WriteMessage(websocket.TextMessage, []byte("jc.9999j.cn"))
			if err != nil {
				log.Println("发送消息错误:", err)
				return
			}
		case <-interrupt:
			log.Println("接收到中断信号，正在关闭连接...")

			// 关闭WebSocket连接
			err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("关闭连接错误:", err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}

// 处理接收到的消息
// func handleWsMessage(message []byte, lastFilteredData map[string]MarketData) {
func handleWsMessage(message []byte) {
	try := func(data []byte) (map[string]interface{}, error) {
		var result map[string]interface{}
		err := json.Unmarshal(data, &result)
		return result, err
	}

	// 尝试解析消息
	var data map[string]interface{}
	var err error
	var marketData map[string]interface{}

	// 尝试直接解析消息
	data, err = try(message)
	if err == nil && data["market"] != nil {
		// 如果消息包含market字段，则获取market数据
		if marketMap, ok := data["market"].(map[string]interface{}); ok {
			marketData = marketMap
		} else if marketStr, ok := data["market"].(string); ok {
			// 如果market是字符串，尝试解析它
			if err := json.Unmarshal([]byte(marketStr), &marketData); err != nil {
				// 解析错误但不输出日志，静默处理
				return
			}
		}
	} else {
		// 尝试解析message.market
		var marketStr string
		if err := json.Unmarshal(message, &data); err == nil {
			if marketJSON, ok := data["market"].(string); ok {
				marketStr = marketJSON
				if err := json.Unmarshal([]byte(marketStr), &marketData); err != nil {
					// 解析错误但不输出日志，静默处理
					return
				}
			}
		}
	}

	if marketData == nil {
		// 静默处理，不输出日志
		return
	}

	// 筛选出【#上期所#沪金主力】和【#上期所#沪金次主力】的数据
	filteredData := make(map[string]MarketData)
	for key, value := range marketData {
		// if key == "【#上期所#沪金主力】" || key == "【#上期所#沪金次主力】" || key == "【#上期所#沪银主力】" || key == "【#上期所#沪银次主力】" {
		if key == "沪金主力" || key == "沪金次主力" || key == "沪银主力" || key == "沪银次主力" {
			if itemData, ok := value.(map[string]interface{}); ok {
				var item MarketData
				if r, ok := itemData["r"].(string); ok {
					item.R = r
				}
				if s, ok := itemData["s"].(string); ok {
					item.S = s
				}
				if h, ok := itemData["h"].(string); ok {
					item.H = h
				}
				if l, ok := itemData["l"].(string); ok {
					item.L = l
				}
				if t, ok := itemData["t"].(string); ok {
					item.T = t
				}
				filteredData[key] = item
				// lastFilteredData[key] = item // 更新最新数据
			}
		}
	}

	// 显示筛选后的数据
	if len(filteredData) > 0 {
		// displayWsFilteredData(filteredData)
		// 将筛选后的数据通过UDP发送
		sendWsDataViaUDP(filteredData)
	}
}

// 显示筛选后的数据
func displayWsFilteredData(filteredData map[string]MarketData) {
	wsLogIfEnabled("\n筛选后的数据:")
	wsLogIfEnabled("--------------------")
	for key, item := range filteredData {
		wsLogIfEnabled(fmt.Sprintf("名称: %s", key))
		wsLogIfEnabled(fmt.Sprintf("回购价: %s", item.R))
		wsLogIfEnabled(fmt.Sprintf("销售价: %s", item.S))
		wsLogIfEnabled(fmt.Sprintf("最高价: %s", item.H))
		wsLogIfEnabled(fmt.Sprintf("最低价: %s", item.L))
		wsLogIfEnabled(fmt.Sprintf("更新时间: %s", item.T))
		wsLogIfEnabled("--------------------")
	}
}

// 通过UDP发送数据
func sendWsDataViaUDP(filteredData map[string]MarketData) {
	// 使用 "|" 分隔符截取字符串
	parts := strings.Split(wsConfig.UDPServer, "|")

	// 遍历筛选后的数据
	for key, item := range filteredData {
		// 构建UDP数据
		timestamp := time.Now().Unix()
		code := "GOLD" // 默认代码
		// 先判断是否为次主力，因为次主力也包含主力字符串
		if strings.Contains(key, "沪金次主力") {
			code = "GOLD2"
		} else if strings.Contains(key, "沪金主力") {
			code = "GOLD"
		} else if strings.Contains(key, "沪银次主力") {
			code = "SILVER2"
		} else if strings.Contains(key, "沪银主力") {
			code = "SILVER"
		}

		processPrice := func(priceStr string) string {
			if strings.Contains(key, "沪银") {
				if priceStr == "" || priceStr == "0" {
					return "0"
				}
				price, err := strconv.ParseFloat(priceStr, 64)
				if err != nil {
					return "0"
				}
				return strconv.FormatFloat(price/1000.0, 'f', -1, 64)
			}
			return priceStr
		}

		var data = WebsocketUdpdata{
			Code:      code,
			Volume:    "0",
			QuoteTime: timestamp,
			Last:      processPrice(item.S), // 使用销售价作为Last
			Open:      "0",
			High:      processPrice(item.H),
			Low:       processPrice(item.L),
			LastClose: "0",
			Buy:       processPrice(item.R), // 使用回购价作为Buy
			Sell:      processPrice(item.S), // 使用销售价作为Sell
		}

		// 将结构体转换为 JSON
		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Println("JSON 序列化错误:", err)
			continue
		}

		// 打印生成的 JSON 数据（可选）
		wsLogIfEnabled("发送的 JSON 数据:", string(jsonData))

		// 输出结果
		for _, part := range parts {
			wsLogIfEnabled("udpserver：", part)
			// 发送 UDP 数据
			addr := part // 目标地址（IP:端口）
			conn, err := net.Dial("udp", addr)
			if err != nil {
				log.Println("UDP 连接错误:", err)
				continue
			}

			_, err = conn.Write(jsonData)
			if err != nil {
				log.Println("发送数据错误:", err)
				conn.Close()
				continue
			} else {
				wsLogIfEnabled("UDP 数据发送成功!")
			}
			conn.Close()
		}
	}
}

// 条件日志输出
func wsLogIfEnabled(v ...interface{}) {
	if wsConfig.Output {
		fmt.Println(v...)
	}
}
