package lib

import (
	"encoding/json"
	"log"
	"os"
	"sync"
)

type Config struct {
	WSURL  string `json:"ws_url"`
	Output bool   `json:"output"`
	Mysql  struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"database"`
		Charset  string `json:"charset"`
	} `json:"mysql"`
	TimerNum  int    `json:"timer_num"`
	UDPServer string `json:"udp_server"`
}

var instance *Config
var once sync.Once

func GetConfig() *Config {
	once.Do(func() {
		instance = loadConfig("configs/config.json")
	})
	return instance
}

func loadConfig(path string) *Config {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Error opening config file: %v", err)
	}
	defer file.Close()

	var config Config
	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		log.Fatalf("Error decoding config file: %v", err)
	}

	return &config
}
