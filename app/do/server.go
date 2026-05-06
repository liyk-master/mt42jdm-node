package do

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // 导入 MySQL 驱动
	"github.com/liyk-master/mt42jdm-node/app/lib"
	"log"
	"net"
	"strings"
	"time"
)

type Udpdata struct {
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

var config *lib.Config

func Do(cfg *lib.Config) {
	config = cfg
	// 查询mysql中的数据
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.Mysql.User, cfg.Mysql.Password, cfg.Mysql.Host, cfg.Mysql.Port, cfg.Mysql.Database)

	// 打开数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("数据库连接失败:", err)
	}
	defer db.Close() // 程序结束时关闭数据库连接

	// 检查数据库连接是否正常
	err = db.Ping()
	if err != nil {
		log.Fatal("无法连接到数据库:", err)
	}

	fmt.Println("数据库连接成功!")

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// 初次立即执行
	go queryWithTimeout(db, cfg)

	// 每 30 秒执行一次查询
	for range ticker.C {
		go queryWithTimeout(db, cfg)
	}
}

func queryWithTimeout(db *sql.DB, cfg *lib.Config) {
	// 使用 context 控制超时，设置超时时间为 10 秒
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := "SELECT SYMBOL, BID, ASK ,LOW, HIGH FROM MT4_PRICES WHERE SYMBOL = ? OR SYMBOL = ?"

	logIfEnabled("执行查询...")
	// 使用 QueryContext 支持上下文超时的查询
	rows, err := db.QueryContext(ctx, query, "BTCUSD", "USOil")
	if err != nil {
		log.Println("查询失败:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var symbol, bid, ask, low, high string
		if err := rows.Scan(&symbol, &bid, &ask, &low, &high); err != nil {
			log.Println("结果扫描失败:", err)
			return
		}
		// 把数据组成json格式通过udp发送出去
		timestamp := time.Now().Unix()
		var data = Udpdata{
			Code:      strings.ToUpper(symbol),
			Volume:    "0",
			QuoteTime: timestamp,
			Last:      ask,
			Open:      "0",
			High:      high,
			Low:       low,
			LastClose: "0",
			Buy:       bid,
			Sell:      ask,
		}
		// 将结构体转换为 JSON
		jsonData, err := json.Marshal(data)
		if err != nil {
			fmt.Println("JSON 序列化错误:", err)
			return
		}

		// 打印生成的 JSON 数据（可选）
		logIfEnabled("发送的 JSON 数据:", string(jsonData))

		// 使用 "|" 分隔符截取字符串
		parts := strings.Split(cfg.UDPServer, "|")

		// 输出结果
		for _, part := range parts {
			logIfEnabled("udpserver：", part)
			// 发送 UDP 数据
			addr := part // 目标地址（IP:端口）
			conn, err := net.Dial("udp", addr)
			if err != nil {
				log.Println("UDP 连接错误:", err)
				return
			}
			defer conn.Close()

			_, err = conn.Write(jsonData)
			if err != nil {
				fmt.Println("发送数据错误:", err)
			} else {
				logIfEnabled("UDP 数据发送成功!")
			}
		}
	}

	if err := rows.Err(); err != nil {
		log.Println("遍历结果时出错:", err)
	}
}

func logIfEnabled(v ...interface{}) {
	if config.Output {
		fmt.Println(v...)
	}
}
