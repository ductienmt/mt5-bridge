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

type fileLogger struct {
	mu   sync.Mutex
	file *os.File
}

var (
	httpClient = &http.Client{Timeout: 10 * time.Second}
	forwardURL string
	lg         = newLogger()
	fl         *fileLogger
)

func main() {
	forwardURL = os.Getenv("FORWARD_TO_URL")
	if forwardURL == "" {
		lg.error("FORWARD_TO_URL not set — exiting")
		os.Exit(1)
	}
	forwardURL = strings.TrimSuffix(forwardURL, "/")
	lg.info("FORWARD_TO_URL = %s", forwardURL)

	fl = newFileLogger()
	if fl == nil {
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
		if fl != nil {
			fl.close()
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

		respBody, httpStatus, err := forwardSignal(sig)
		latency := time.Since(sentAt)

		if err != nil {
			lg.error("forward error: %v", err)
			fl.append(sig, sentAt, "", httpStatus, err.Error(), latency)
			conn.Write([]byte(fmt.Sprintf(`{"ok":false,"error":"%v"}`+"\n", err)))
			continue
		}

		lg.order(&sig, httpStatus, latency)
		fl.append(sig, sentAt, string(respBody), httpStatus, "", latency)
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

// ─── File Logger ─────────────────────────────────────────────────────────

func newFileLogger() *fileLogger {
	logPath := getEnv("ORDER_LOG_PATH", "/app/logs/orders.log")
	dir := strings.TrimSuffix(logPath, "/orders.log")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil
	}
	return &fileLogger{file: f}
}

func (l *fileLogger) append(sig Signal, sentAt time.Time, respBody string, httpStatus int, errMsg string, latency time.Duration) {
	if l == nil {
		return
	}

	ts := sentAt.Format(time.RFC3339)
	status := fmt.Sprintf("OK:%d", httpStatus)
	if errMsg != "" {
		status = "ERROR:" + errMsg
	}

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

	l.mu.Lock()
	defer l.mu.Unlock()
	l.file.WriteString(line)
	l.file.Sync()
}

func (l *fileLogger) close() {
	if l == nil {
		return
	}
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
	switch action {
	case "OPEN":
		switch side {
		case "BUY", "BUY_STOP", "BUY_LIMIT":
			color = "\x1b[32m"
		case "SELL", "SELL_STOP", "SELL_LIMIT":
			color = "\x1b[31m"
		default:
			color = "\x1b[34m"
		}
	case "CLOSE":
		color = "\x1b[35m"
	case "EDIT":
		color = "\x1b[33m"
	default:
		color = "\x1b[34m"
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprintf(os.Stdout,
		"[%s] %sORDER\x1b[0m %s%-6s %s\x1b[0m lot=%.2f @ %.5f | SL=%.5f TP=%.5f | magic=%d | pnl=%+.2f | resp=%d | %s\n",
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
