package do

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

type wsURLPool struct {
	mu   sync.RWMutex
	urls []string
}

func (p *wsURLPool) Set(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.urls = append([]string(nil), urls...)
}

func (p *wsURLPool) GetAll() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]string(nil), p.urls...)
}

func getWSHost(cfg *lib.Config) string {
	if strings.TrimSpace(cfg.WSHost) != "" {
		return strings.TrimSpace(cfg.WSHost)
	}
	return "www.9999j.cn"
}

func getWSDiscoverURL(cfg *lib.Config) string {
	if strings.TrimSpace(cfg.WSDiscoverURL) != "" {
		return strings.TrimSpace(cfg.WSDiscoverURL)
	}
	return "http://www.9999j.cn/m2"
}

func getWSRefreshSec(cfg *lib.Config) time.Duration {
	if cfg.WSRefreshSec > 0 {
		return time.Duration(cfg.WSRefreshSec) * time.Second
	}
	return 300 * time.Second
}

func getWSReconnectSec(cfg *lib.Config) time.Duration {
	if cfg.WSReconnectSec > 0 {
		return time.Duration(cfg.WSReconnectSec) * time.Second
	}
	return 3 * time.Second
}

func fetchWSURLs(cfg *lib.Config) ([]string, error) {
	resp, err := http.Get(getWSDiscoverURL(cfg))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discover status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile(`var\s+domains\s*=\s*'([^']+)'`)
	matches := re.FindStringSubmatch(string(body))
	if len(matches) < 2 {
		return nil, fmt.Errorf("domains not found in discover page")
	}

	host := getWSHost(cfg)
	rawDomains := strings.Split(matches[1], ",")
	seen := make(map[string]struct{})
	urls := make([]string, 0, len(rawDomains))
	for _, d := range rawDomains {
		domain := strings.TrimSpace(d)
		if domain == "" {
			continue
		}
		url := fmt.Sprintf("ws://%s/ws/market/%s", domain, host)
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		urls = append(urls, url)
	}

	if len(urls) == 0 {
		return nil, fmt.Errorf("no websocket urls generated")
	}

	return urls, nil
}

func refreshWSURLs(pool *wsURLPool, cfg *lib.Config) {
	urls, err := fetchWSURLs(cfg)
	if err != nil {
		log.Println("刷新WS地址失败:", err)
		return
	}
	pool.Set(urls)
	wsLogIfEnabled("WS地址刷新成功，数量:", len(urls))
}

func runWSSession(c *websocket.Conn, interrupt <-chan os.Signal) error {
	done := make(chan error, 1)

	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				done <- err
				return
			}
			handleWsMessage(message)
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			return err
		case <-ticker.C:
			if err := c.WriteMessage(websocket.TextMessage, []byte("jc.9999j.cn")); err != nil {
				return err
			}
		case <-interrupt:
			_ = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return nil
		}
	}
}

// DoWebsocket 启动WebSocket连接和数据处理
func DoWebsocket(cfg *lib.Config) {
	wsConfig = cfg

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	pool := &wsURLPool{}
	if urls, err := fetchWSURLs(cfg); err == nil {
		pool.Set(urls)
		wsLogIfEnabled("初始化WS地址成功，数量:", len(urls))
	} else {
		log.Println("初始化抓取WS地址失败:", err)
		if strings.TrimSpace(wsConfig.WSURL) != "" {
			pool.Set([]string{strings.TrimSpace(wsConfig.WSURL)})
			log.Println("使用配置ws_url作为兜底地址")
		}
	}

	go func() {
		ticker := time.NewTicker(getWSRefreshSec(cfg))
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				refreshWSURLs(pool, cfg)
			case <-interrupt:
				return
			}
		}
	}()

	reconnectDelay := getWSReconnectSec(cfg)

	for {
		select {
		case <-interrupt:
			log.Println("接收到中断信号，停止WebSocket处理")
			return
		default:
		}

		urls := pool.GetAll()
		if len(urls) == 0 {
			log.Println("当前无可用WS地址，等待后重试")
			time.Sleep(reconnectDelay)
			refreshWSURLs(pool, cfg)
			continue
		}

		connected := false
		for _, wsURL := range urls {
			log.Println("正在连接到WebSocket服务器:", wsURL)
			c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				log.Println("连接失败:", err)
				continue
			}

			connected = true
			log.Println("连接成功:", wsURL)
			err = runWSSession(c, interrupt)
			c.Close()

			if err == nil {
				return
			}
			log.Println("连接中断，准备切换下一个地址:", err)
		}

		if !connected {
			log.Println("本轮所有WS地址连接失败")
		}
		time.Sleep(reconnectDelay)
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
