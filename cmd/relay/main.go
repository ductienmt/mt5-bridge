// Relay: nhận signal qua TCP rồi forward lên server HTTP khác
// MT5 EA → TCP Bridge → Relay → http://103.72.56.53:8080
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Signal struct {
	Action   string  `json:"action"`
	Side    string  `json:"side"`
	Symbol  string  `json:"symbol"`
	Lot     float64 `json:"lot"`
	Price   float64 `json:"price"`
	SL      float64 `json:"sl"`
	TP      float64 `json:"tp"`
	Magic   int64   `json:"magic"`
	Pnl     float64 `json:"pnl"`
	Comment string  `json:"comment"`
}

type orderLog struct {
	mu     sync.Mutex
	file   *os.File
	path   string
}

var (
	httpClient = &http.Client{Timeout: 10 * time.Second}
	forwardURL string
	lg        = newLogger()
	orderLog    *orderLog
)

func main() {
	forwardURL = os.Getenv("FORWARD_TO_URL")
	if forwardURL == "" {
		lg.error("FORWARD_TO_URL not set — exiting")
		os.Exit(1)
	}
	forwardURL = strings.TrimSuffix(forwardURL, "/")
	lg.info("FORWARD_TO_URL = %s", forwardURL)

	orderLog = newOrderLog("/app/logs/orders.log")
	if orderLog == nil {
		lg.warn("Order log disabled (write permission denied)")
	}

	port := getEnv("RELAY_PORT", "8082")
	addr := ":" + port

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		lg.error("Bind failed on %s: %v", addr, err)
		os.Exit(1)
	}
	lg.info("Relay listening on %s -> %s", addr, forwardURL)

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		lg.info("Shutting down relay...")
		ln.Close()
		if orderLog != nil {
			orderLog.close()
		}
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

func handleConn(conn net.Conn) {
	defer conn.Close()
	addr := conn.RemoteAddr().String()

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

		sigRaw := string(line)
		sentAt := time.Now()

		// Forward lên HTTP server
		respBody, httpStatus, err := forwardSignal(sig)
		recvAt := time.Now()
		latency := recvAt.Sub(sentAt)

		if err != nil {
			lg.error("[%s] forward error: %v", addr, err)
			orderLog.append(sig, sentAt, "", httpStatus, err.Error(), latency)
			conn.Write([]byte(fmt.Sprintf(`{"ok":false,"error":"%v"}`+"\n", err)))
			continue
		}

		// Log ra console + file
		lg.order(&sig, httpStatus, latency)
		orderLog.append(sig, sentAt, string(respBody), httpStatus, "", latency)

		conn.Write(append(respBody, '\n'))
	}
}

func forwardSignal(sig Signal) ([]byte, int, error) {
	body, err := json.Marshal(sig)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequest(http.MethodPost, forwardURL+"/signal", bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, resp.StatusCode, err
	}

	if resp.StatusCode >= 400 {
		return data, resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	return data, resp.StatusCode, nil
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

// ─── Order Log ───────────────────────────────────────────────────────────

func newOrderLog(path string) *orderLog {
	dir := strings.TrimSuffix(path, "/orders.log")
	if dir == path {
		dir = "."
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil
	}
	// Lưu log tại /app/orders.log (được mount ra host)
	path := getEnv("ORDER_LOG_PATH", "/app/orders.log")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil
	}
	return &orderLog{file: f, path: path}
}

func (l *orderLog) append(sig Signal, sentAt time.Time, respBody string, httpStatus int, errMsg string, latency time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ts := sentAt.Format(time.RFC3339)

	var status string
	if errMsg != "" {
		status = fmt.Sprintf("ERROR:%s", errMsg)
	} else {
		status = fmt.Sprintf("OK:%d", httpStatus)
	}

	// CSV: ts, action, side, symbol, lot, price, sl, tp, magic, pnl, comment, status, latency_ms, resp_body
	line := fmt.Sprintf(
		"%s|%s|%s|%s|%.2f|%.5f|%.5f|%.5f|%d|%.2f|%s|%s|%.0f|%s\n",
		ts,
		strings.ToUpper(sig.Action),
		strings.ToUpper(sig.Side),
		strings.ToUpper(sig.Symbol),
		sig.Lot,
		sig.Price,
		sig.SL,
		sig.TP,
		sig.Magic,
		sig.Pnl,
		sig.Comment,
		status,
		float64(latency.Microseconds())/1000.0,
		strings.TrimSpace(respBody),
	)

	l.file.WriteString(line)
	l.file.Sync()
}

func (l *orderLog) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		l.file.Close()
	}
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

func (l *logger) order(s *Signal, httpStatus int, latency time.Duration) {
	action := strings.ToUpper(s.Action)
	side := strings.ToUpper(s.Side)

	var color string
	var ok bool
	switch action {
	case "OPEN":
		switch side {
		case "BUY", "BUY_STOP", "BUY_LIMIT":
			color, ok = "\x1b[32m", true
		case "SELL", "SELL_STOP", "SELL_LIMIT":
			color, ok = "\x1b[31m", true
		default:
			color = "\x1b[34m"
		}
	case "CLOSE":
		color = "\x1b[35m"
		ok = true
	case "EDIT":
		color = "\x1b[33m"
		ok = true
	default:
		color = "\x1b[34m"
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprintf(os.Stdout,
		"[%s] %sORDER\x1b[0m %s%-6s %s\x1b[0m %-8s lot=%.2f @ %.5f | SL=%.5f TP=%.5f | magic=%d | pnl=%+.2f | resp=%d | %s\n",
		time.Now().Format("15:04:05"),
		color,
		action, side,
		"\x1b[37m"+s.Symbol+"\x1b[0m",
		s.Lot, s.Price,
		s.SL, s.TP,
		s.Magic,
		s.Pnl,
		httpStatus,
		latency.Round(time.Millisecond),
	)
}
