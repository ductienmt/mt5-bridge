// TCP bridge: Go server nhận lệnh qua TCP rồi đẩy vào queue
// → MT5 EA TradingBridgeSocketEA đọc qua DLL
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	sigpkg "mt5-bridge/signal"
)

var q  = sigpkg.Get()
var lg = sigpkg.NewLogger()

type Client struct {
	conn net.Conn
	addr string
	seen time.Time
}

var (
	clients   = make(map[net.Conn]*Client)
	clientsMu sync.RWMutex
)

func main() {
	sigpkg.Banner()

	port := getEnv("MT5_TCP_PORT", "8081")
	addr := ":" + port

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		lg.Error("Bind failed on %s: %v", addr, err)
		os.Exit(1)
	}

	lg.Info("TCP bridge listening on %s", addr)
	lg.Info("Waiting for clients (TradingBridgeSocketEA)...")

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		lg.Info("Shutting down TCP bridge...")
		ln.Close()
		clientsMu.Lock()
		for c := range clients {
			c.Close()
		}
		clientsMu.Unlock()
		os.Exit(0)
	}()

	var wg sync.WaitGroup

	for {
		conn, err := ln.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "closed") {
				break
			}
			lg.Error("Accept error: %v", err)
			continue
		}

		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			handleConn(c)
		}(conn)
	}

	wg.Wait()
	lg.Info("Server stopped")
}

func handleConn(conn net.Conn) {
	defer func() {
		clientsMu.Lock()
		delete(clients, conn)
		clientsMu.Unlock()
		conn.Close()
	}()

	addr := conn.RemoteAddr().String()
	clientsMu.Lock()
	clients[conn] = &Client{conn: conn, addr: addr, seen: time.Now()}
	clientsMu.Unlock()

	lg.Info("Client connected: %s", addr)
	lg.Info("Active connections: %d", func() int {
		clientsMu.RLock()
		defer clientsMu.RUnlock()
		return len(clients)
	}())

	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		clientsMu.Lock()
		if c, ok := clients[conn]; ok {
			c.seen = time.Now()
		}
		clientsMu.Unlock()

		raw := string(line)

		// Try JSON first
		var rawSig sigpkg.RawSignal
		if err := json.Unmarshal(line, &rawSig); err != nil {
			sig, ok := parsePipe(raw)
			if !ok {
				lg.Warn("Invalid message from %s: %s", addr, raw)
				continue
			}
			q.Push(sig)
			lg.Order(&sig)
			continue
		}

		sig, err := sigpkg.ParseSignal(line)
		if err != nil {
			lg.Warn("Parse error from %s: %v", addr, err)
			continue
		}

		q.Push(sig)
		lg.Order(&sig)

		ack := map[string]any{
			"status": "queued",
			"queue":  q.Size(),
			"time":   time.Now().Format(time.RFC3339),
		}
		ackBytes, _ := json.Marshal(ack)
		conn.Write(append(ackBytes, '\n'))
	}

	if err := scanner.Err(); err != nil {
		if _, ok := err.(*net.OpError); !ok && err != io.EOF {
			lg.Error("Connection error [%s]: %v", addr, err)
		}
	}

	lg.Info("Client disconnected: %s", addr)
}

func parsePipe(raw string) (sigpkg.Signal, bool) {
	parts := strings.Split(strings.TrimSpace(raw), "|")
	if len(parts) < 2 {
		return sigpkg.Signal{}, false
	}
	sig := sigpkg.Signal{Action: parts[0], Symbol: parts[1], Time: time.Now()}
	if len(parts) > 2 { fmt.Sscanf(parts[2], "%f", &sig.Lot) }
	if len(parts) > 3 { fmt.Sscanf(parts[3], "%f", &sig.Price) }
	if len(parts) > 4 { fmt.Sscanf(parts[4], "%f", &sig.SL) }
	if len(parts) > 5 { fmt.Sscanf(parts[5], "%f", &sig.TP) }
	if len(parts) > 6 { fmt.Sscanf(parts[6], "%d", &sig.Magic) }
	if len(parts) > 7 { sig.Comment = parts[7] }
	return sig, true
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
