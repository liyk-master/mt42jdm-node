# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development commands

- Run app:
  - `go run .`
- Build binary:
  - `go build .`
- Run all tests:
  - `go test ./...`
- Run tests for one package:
  - `go test ./app/do`
- Run a single named test in one package:
  - `go test ./app/do -run TestName`

Note: the current codebase has no `*_test.go` files yet, so test commands currently report `[no test files]`.

## High-level architecture

This is a small Go market-data forwarding service with two ingestion paths that normalize quotes and send them over UDP.

### Runtime flow

- Entry point: `main.go`
  - Loads config once via `app/lib.GetConfig()`.
  - Starts WebSocket ingestion by default with `do.DoWebsocket(cfg)`.
  - Keeps MySQL polling path available but disabled (`do.Do(cfg)` is commented).

### Configuration

- Config loader: `app/lib/config.go`
  - Reads `configs/config.json` once via `sync.Once` singleton.
  - Key runtime fields:
    - `ws_url`: upstream WebSocket endpoint
    - `output`: enables verbose console logging
    - `udp_server`: one or more UDP targets (pipe-separated in code)
    - optional MySQL settings used by polling mode

### Data ingestion and forwarding

- WebSocket path (active): `app/do/websocket.go`
  - Connects to upstream WS URL from config.
  - Sends heartbeat/subscription text (`jc.9999j.cn`) every second.
  - Parses incoming JSON payloads (supports `market` as object or nested JSON string).
  - Filters instruments to gold/silver main & secondary contracts.
  - Normalizes values into a UDP payload struct and sends JSON via UDP.
  - Applies silver-price scaling (`/1000`) before emit.

- MySQL polling path (optional): `app/do/server.go`
  - Connects to MySQL using config DSN.
  - Polls `MT4_PRICES` on a ticker with context timeout.
  - Maps rows to same UDP JSON shape and forwards to configured UDP targets.

### Output contract

Both ingestion paths emit JSON with fields:
`Code, Volume, QuoteTime, Last, Open, High, Low, LastClose, Buy, Sell`.

Downstream consumers should treat this as the stable transport schema for quote updates.

## 语言规范
- 所有对话和文档都使用中文
- 文档使用 markdown 格式