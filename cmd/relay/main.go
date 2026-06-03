// Relay: nhận signal qua TCP rồi forward lên server TCP khác
// TCP Bridge → Relay :1082 → TCP Server
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
)

// Signal là input nhận từ tcp-bridge (từ MT5 EA)
// action = OPEN/CLOSE/EDIT, side = BUY/SELL
type Signal struct {
	Action   string  `json:"action"`
	Side     string  `json:"side"`
	Symbol   string  `json:"symbol"`
	Lot      float64 `json:"lot"`
	Price    float64 `json:"price"`
	SL       float64 `json:"sl"`
	TP       float64 `json:"tp"`
	Magic    int64   `json:"magic"`
	Pnl      float64 `json:"pnl"`
	Comment  string  `json:"comment"`
}

// TcpPayload là payload gửi tới server TCP đích
// action = BUY/SELL/BUY_STOP/SELL_STOP/CLOSE/MODIFY/CLOSE_ALL (thứ server cần)
type TcpPayload struct {
	Action  string  `json:"action"`
	Symbol  string  `json:"symbol"`
	Lot     float64 `json:"lot"`
	SL      float64 `json:"sl"`
	TP      float64 `json:"tp"`
	Magic   int64   `json:"magic"`
	Pnl     float64 `json:"pnl"`
	Comment string  `json:"comment"`
}

// effectiveAction derives the action string the server expects:
// - OPEN + BUY  → BUY  (market, price=0) or BUY + price (limit)
// - OPEN + SELL → SELL (market, price=0) or SELL + price (limit)
// - CLOSE       → CLOSE
// - EDIT        → MODIFY
func effectiveAction(s *Signal) string {
	switch s.Action {
	case "CLOSE":
		return "CLOSE"
	case "EDIT":
		return "MODIFY"
	case "OPEN":
		return s.Side
	}
	return ""
}

func signalToTcpPayload(s *Signal) *TcpPayload {
	return &TcpPayload{
		Action:  effectiveAction(s),
		Symbol:  s.Symbol,
		Lot:     s.Lot,
		SL:      s.SL,
		TP:      s.TP,
		Magic:   s.Magic,
		Pnl:     s.Pnl,
		Comment: s.Comment,
	}
}

var (
	lg            = newLogger()
	forwardHost   string
	forwardPort   string
	serverConn    net.Conn
	serverMu      sync.Mutex
	serverClose   = make(chan struct{})
)

func main() {
	forwardHost = os.Getenv("FORWARD_TO_HOST")
	forwardPort = os.Getenv("FORWARD_TO_PORT")
	if forwardHost == "" || forwardPort == "" {
		lg.error("FORWARD_TO_HOST and FORWARD_TO_PORT must be set — exiting")
		os.Exit(1)
	}
	lg.info("FORWARD_TO = %s:%s", forwardHost, forwardPort)

	// Start TCP connection to forward server
	go keepServerConnection()

	port := getEnv("RELAY_PORT", "8082")
	addr := ":" + port

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		lg.error("Bind failed on %s: %v", addr, err)
		os.Exit(1)
	}
	lg.info("Relay listening on %s -> %s:%s", addr, forwardHost, forwardPort)

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		lg.info("Shutting down relay...")
		close(serverClose)
		if serverConn != nil {
			serverConn.Close()
		}
		ln.Close()
		os.Exit(0)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "closed") {
				break
			}
			lg.error("Accept error: %v", err)
			continue
		}
		go handleConn(conn)
	}
}

// keepServerConnection giữ kết nối TCP tới server, tự reconnect nếu rớt.
func keepServerConnection() {
	pingInterval := 30 * time.Second // Ping mỗi 30s để giữ connection alive
	addr := fmt.Sprintf("%s:%s", forwardHost, forwardPort)

	for {
		select {
		case <-serverClose:
			return
		case <-reconnectServer:
			// Reconnect được trigger từ sendToServer khi connection bị đóng
		default:
		}

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			lg.warn("Cannot connect to server %s: %v — retry in 5s", addr, err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Tắt Nagle's Algorithm để gói tin được gửi ngay lập tức
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true)
		}

		serverMu.Lock()
		serverConn = conn
		serverMu.Unlock()

		lg.info("Connected to server %s", addr)

		// Giữ connection alive bằng ping định kỳ
		pingTicker := time.NewTicker(pingInterval)
		defer pingTicker.Stop()

		for {
			select {
			case <-serverClose:
				conn.Close()
				return
			case <-reconnectServer:
				conn.Close()
				goto reconnectLoop
			case <-pingTicker.C:
				// Gửi ping nhẹ để giữ connection alive
				serverMu.Lock()
				currentConn := serverConn
				serverMu.Unlock()

				if currentConn != conn {
					conn.Close()
					goto reconnectLoop
				}

				// Thử write một byte null để check connection
				conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				_, err := conn.Write([]byte{})
				if err != nil {
					lg.warn("Server connection lost (keep-alive): %v", err)
					conn.Close()
					goto reconnectLoop
				}
			}
		}
	reconnectLoop:
	}
}

// reconnectServer dùng để notify cho keepServerConnection biết cần reconnect
var reconnectServer = make(chan struct{}, 1)

// sendToServer forward signal sang server.
// Nếu chưa có connection hoặc connection bị đóng, dial mới ngay lập tức.
func sendToServer(line []byte) error {
	addr := fmt.Sprintf("%s:%s", forwardHost, forwardPort)

	// Thử gửi với connection hiện tại
	for attempt := 0; attempt < 3; attempt++ {
		serverMu.Lock()
		conn := serverConn
		serverMu.Unlock()

		if conn != nil {
			// Gửi plain JSON newline-delimited
			data := append(line, '\n')
			_, err := conn.Write(data)
			if err != nil {
				// Connection bị đóng, trigger reconnect và dial mới
				serverMu.Lock()
				if serverConn != nil {
					serverConn.Close()
					serverConn = nil
				}
				serverMu.Unlock()

				// Signal cho keepServerConnection biết cần reconnect
				select {
				case reconnectServer <- struct{}{}:
				default:
				}
			} else {
				// Thành công!
				return nil
			}
		}

		// Chưa có connection, dial mới ngay
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			lg.warn("Dial failed: %v — retry in 500ms", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// Tắt Nagle's Algorithm
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true)
		}

		serverMu.Lock()
		serverConn = conn
		serverMu.Unlock()

		lg.info("Connected to server %s (send)", addr)

		// Thử gửi ngay với connection mới
		data := append(line, '\n')
		_, err = conn.Write(data)
		if err != nil {
			serverMu.Lock()
			if serverConn != nil {
				serverConn.Close()
				serverConn = nil
			}
			serverMu.Unlock()

			select {
			case reconnectServer <- struct{}{}:
			default:
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		return nil
	}

	return fmt.Errorf("failed after 3 attempts")
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	peer := conn.RemoteAddr().String()

	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var sig Signal
		if err := json.Unmarshal(line, &sig); err != nil {
			sig = parsePipe(string(line))
		}

		if sig.Action == "" && sig.Side == "" {
			conn.Write([]byte(`{"ok":false,"error":"invalid signal"}` + "\n"))
			continue
		}

		sentAt := time.Now()

		// ── RECV ──────────────────────────────────────────
		lg.received(peer, &sig)

		// Build payload for TCP: chuyển action=OPEN/side=BUY thành action=BUY
		payload := signalToTcpPayload(&sig)
		payloadBytes, _ := json.Marshal(payload)

		// Forward tới server TCP
		err := sendToServer(payloadBytes)
		latency := time.Since(sentAt)

		if err != nil {
			lg.error("FWD->%s:%s FAIL %s %s %s %.2f | %v",
				forwardHost, forwardPort, sig.Action, payload.Action, payload.Symbol, payload.Lot, err)
			conn.Write([]byte(fmt.Sprintf(`{"ok":false,"error":"%v"}`+"\n", err)))
			continue
		}

		// ── SENT ──────────────────────────────────────────
		lg.sent(forwardHost, payload, latency)
		conn.Write([]byte(`{"ok":true}` + "\n"))
	}
}

func parsePipe(raw string) Signal {
	parts := strings.Split(strings.TrimSpace(raw), "|")
	if len(parts) < 2 {
		return Signal{}
	}
	sig := Signal{Action: parts[0], Side: parts[1]}
	if len(parts) > 2 {
		sig.Symbol = parts[2]
	}
	if len(parts) > 3 {
		fmt.Sscanf(parts[3], "%f", &sig.Lot)
	}
	if len(parts) > 4 {
		fmt.Sscanf(parts[4], "%f", &sig.Price)
	}
	if len(parts) > 5 {
		fmt.Sscanf(parts[5], "%f", &sig.SL)
	}
	if len(parts) > 6 {
		fmt.Sscanf(parts[6], "%f", &sig.TP)
	}
	if len(parts) > 7 {
		fmt.Sscanf(parts[7], "%d", &sig.Magic)
	}
	if len(parts) > 8 {
		fmt.Sscanf(parts[8], "%f", &sig.Pnl)
	}
	if len(parts) > 9 {
		sig.Comment = parts[9]
	}
	return sig
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Console Logger ─────────────────────────────────────────────────────

type logger struct {
	mu sync.Mutex
}

func newLogger() *logger { return &logger{} }

func (l *logger) info(format string, args ...interface{})  { l.log("[INFO] ", fmt.Sprintf(format, args...)) }
func (l *logger) error(format string, args ...interface{}) { l.log("[ERROR]", fmt.Sprintf(format, args...)) }
func (l *logger) warn(format string, args ...interface{})  { l.log("[WARN] ", fmt.Sprintf(format, args...)) }

func (l *logger) log(prefix, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Printf("[%s] %s %s\n", time.Now().Format("2006-01-02 15:04:05"), prefix, msg)
}

func (l *logger) received(from string, s *Signal) {
	l.mu.Lock()
	defer l.mu.Unlock()

	_ = from
	_ = s
}

func (l *logger) sent(to string, p *TcpPayload, latency time.Duration) {
	var color string
	switch strings.ToUpper(p.Action) {
	case "BUY", "BUY_STOP":
		color = "\x1b[32m"
	case "SELL", "SELL_STOP":
		color = "\x1b[31m"
	case "CLOSE", "CLOSE_ALL":
		color = "\x1b[35m"
	case "MODIFY":
		color = "\x1b[33m"
	default:
		color = "\x1b[34m"
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	comment := ""
	if p.Comment != "" {
		comment = " | " + p.Comment
	}

	fmt.Printf(
		"[%s] \x1b[36mSENT\x1b[0m \x1b[90m-> %s:%s\x1b[0m | %s%s %s %.2f | SL=%.5f TP=%.5f | magic=%d | pnl=%+.2f | %s%s\n",
		time.Now().Format("15:04:05"),
		to, forwardPort,
		color, strings.ToUpper(p.Action),
		p.Symbol,
		p.Lot,
		p.SL,
		p.TP,
		p.Magic,
		p.Pnl,
		latency.Round(time.Millisecond),
		comment,
	)
}
