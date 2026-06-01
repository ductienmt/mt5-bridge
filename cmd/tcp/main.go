// TCP bridge: nhận lệnh từ MT5 EA qua TCP,
// push vào queue (cho DLL đọc) + forward sang relay (để forward đi server khác)
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	sigpkg "mt5-bridge/signal"
)

var (
	q          = sigpkg.Get()
	lg         = sigpkg.NewLogger()
	relayConn  net.Conn
	relayMu    sync.Mutex
	relayHost  string
	relayClose = make(chan struct{})
)

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

	relayHost = os.Getenv("RELAY_HOST")
	if relayHost != "" {
		lg.Info("RELAY_HOST set: %s — sẽ forward signal sang relay", relayHost)
		go keepRelayConnection()
	}

	port := getEnv("MT5_TCP_PORT", "8081")
	addr := ":" + port

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		lg.Error("Bind failed on %s: %v", addr, err)
		os.Exit(1)
	}

	lg.Info("TCP bridge listening on %s", addr)
	lg.Info("Queue backend ready (DLL đọc từ đây)")

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		lg.Info("Shutting down TCP bridge...")
		close(relayClose)
		if relayConn != nil {
			relayConn.Close()
		}
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

// keepRelayConnection giữ kết nối TCP tới relay, tự reconnect nếu rớt.
func keepRelayConnection() {
	for {
		select {
		case <-relayClose:
			return
		default:
		}

		conn, err := net.DialTimeout("tcp", relayHost, 5*time.Second)
		if err != nil {
			lg.Warn("Cannot connect to relay %s: %v — retry in 5s", relayHost, err)
			time.Sleep(5 * time.Second)
			continue
		}

		relayMu.Lock()
		relayConn = conn
		relayMu.Unlock()

		lg.Info("Connected to relay %s", relayHost)

		// Chờ bị đóng hoặc rớt
		<-relayClose
		return
	}
}

// sendToRelay forward signal sang relay qua TCP connection đang có.
func sendToRelay(line []byte) {
	relayMu.Lock()
	conn := relayConn
	relayMu.Unlock()

	if conn == nil {
		lg.Warn("No relay connection — signal not forwarded")
		return
	}

	// Gửi plain JSON newline-delimited (relay dùng bufio.Scanner)
	data := append(line, '\n')
	_, err := conn.Write(data)
	if err != nil {
		lg.Warn("Failed to forward to relay: %v", err)
		relayMu.Lock()
		if relayConn != nil {
			relayConn.Close()
			relayConn = nil
		}
		relayMu.Unlock()
	}
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

	lg.Info("MT5 EA connected: %s", addr)

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
			tcpLog("RECV", addr, &sig)
			if relayHost != "" {
				sendToRelay(line)
				tcpLog("FWD ", addr, &sig)
			}
			continue
		}

		sig, err := sigpkg.ParseSignal(line)
		if err != nil {
			lg.Warn("Parse error from %s: %v", addr, err)
			continue
		}

		q.Push(sig)
		tcpLog("RECV", addr, &sig)

		if relayHost != "" {
			sendToRelay(line)
			// tcpLog("FWD ", addr, &sig)
		}

		ack := map[string]any{
			"status": "queued",
			"queue":  q.Size(),
			"pnl":    sig.Pnl,
			"time":   time.Now().Format(time.RFC3339),
		}
		ackBytes, _ := json.Marshal(ack)
		conn.Write(append(ackBytes, '\n'))
	}

	if err := scanner.Err(); err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") && err.Error() != "EOF" {
			lg.Error("Connection error [%s]: %v", addr, err)
		}
	}

	lg.Info("MT5 EA disconnected: %s", addr)
}

func parsePipe(raw string) (sigpkg.Signal, bool) {
	parts := strings.Split(strings.TrimSpace(raw), "|")
	if len(parts) < 2 {
		return sigpkg.Signal{}, false
	}
	sig := sigpkg.Signal{Action: parts[0], Side: parts[1], Symbol: parts[2], Time: time.Now()}
	if len(parts) > 3 { fmt.Sscanf(parts[3], "%f", &sig.Lot) }
	if len(parts) > 4 { fmt.Sscanf(parts[4], "%f", &sig.Price) }
	if len(parts) > 5 { fmt.Sscanf(parts[5], "%f", &sig.SL) }
	if len(parts) > 6 { fmt.Sscanf(parts[6], "%f", &sig.TP) }
	if len(parts) > 7 { fmt.Sscanf(parts[7], "%d", &sig.Magic) }
	if len(parts) > 8 { fmt.Sscanf(parts[8], "%f", &sig.Pnl) }
	if len(parts) > 9 { sig.Comment = parts[9] }
	return sig, true
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// tcpLog in ra console chi tiết signal với màu sắc theo action.
func tcpLog(label, from string, s *sigpkg.Signal) {
	var color string
	switch strings.ToUpper(s.Action) {
	case "OPEN":
		if strings.HasPrefix(strings.ToUpper(s.Side), "BUY") {
			color = "\x1b[32m"
		} else {
			color = "\x1b[31m"
		}
	case "CLOSE":
		color = "\x1b[35m"
	case "EDIT":
		color = "\x1b[33m"
	default:
		color = "\x1b[34m"
	}

	comment := ""
	if s.Comment != "" {
		comment = " | " + s.Comment
	}

	fmt.Printf(
		"[%s] %s %s%s %s %.2f @ %.5f | SL=%.5f TP=%.5f | magic=%d | pnl=%+.2f | %s%s\n",
		time.Now().Format("15:04:05"),
		label,
		color, strings.ToUpper(s.Action),
		strings.ToUpper(s.Side),
		s.Lot, s.Price,
		s.SL, s.TP,
		s.Magic,
		s.Pnl,
		from,
		comment,
	)
}
