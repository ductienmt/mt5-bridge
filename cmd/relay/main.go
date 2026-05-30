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
	Action  string  `json:"action"`
	Side   string  `json:"side"`
	Symbol string  `json:"symbol"`
	Lot    float64 `json:"lot"`
	Price  float64 `json:"price"`
	SL     float64 `json:"sl"`
	TP     float64 `json:"tp"`
	Magic  int64   `json:"magic"`
	Pnl    float64 `json:"pnl"`
	Comment string  `json:"comment"`
}

var (
	httpClient = &http.Client{Timeout: 10 * time.Second}
	forwardURL string
	lg         = newLogger()
)

func main() {
	forwardURL = os.Getenv("FORWARD_TO_URL")
	if forwardURL == "" {
		lg.error("FORWARD_TO_URL not set — exiting")
		os.Exit(1)
	}
	forwardURL = strings.TrimSuffix(forwardURL, "/")
	lg.info("FORWARD_TO_URL = %s", forwardURL)

	port := getEnv("RELAY_PORT", "8082")
	addr := ":" + port

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		lg.error("Bind failed on %s: %v", addr, err)
		os.Exit(1)
	}
	lg.info("Relay listening on %s → %s", addr, forwardURL)

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		lg.info("Shutting down relay...")
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

		raw := string(line)
		var sig Signal
		if err := json.Unmarshal(line, &sig); err != nil {
			// Thử pipe format
			sig = parsePipe(raw)
		}

		if sig.Action == "" && sig.Side == "" {
			conn.Write([]byte("{\"ok\":false,\"error\":\"invalid signal\"}\n"))
			continue
		}

		// Forward lên HTTP server
		resp, err := forwardSignal(sig)
		if err != nil {
			lg.error("[%s] forward error: %v", addr, err)
			conn.Write([]byte(fmt.Sprintf(`{"ok":false,"error":"%v"}`+"\n", err)))
			continue
		}

		lg.order(&sig)
		conn.Write(append(resp, '\n'))
	}
}

func forwardSignal(sig Signal) ([]byte, error) {
	body, err := json.Marshal(sig)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, forwardURL+"/signal", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(data))
	}

	return data, nil
}

func parsePipe(raw string) Signal {
	parts := strings.Split(strings.TrimSpace(raw), "|")
	if len(parts) < 2 {
		return Signal{}
	}
	sig := Signal{Action: parts[0], Side: parts[1]}
	if len(parts) > 2 { sig.Symbol = parts[2] }
	if len(parts) > 3 { fmt.Sscanf(parts[3], "%f", &sig.Lot) }
	if len(parts) > 4 { fmt.Sscanf(parts[4], "%f", &sig.Price) }
	if len(parts) > 5 { fmt.Sscanf(parts[5], "%f", &sig.SL) }
	if len(parts) > 6 { fmt.Sscanf(parts[6], "%f", &sig.TP) }
	if len(parts) > 7 { fmt.Sscanf(parts[7], "%d", &sig.Magic) }
	if len(parts) > 8 { fmt.Sscanf(parts[8], "%f", &sig.Pnl) }
	if len(parts) > 9 { sig.Comment = parts[9] }
	return sig
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Logger ───────────────────────────────────────────────────────────────

type logger struct {
	mu sync.Mutex
}

func newLogger() *logger { return &logger{} }

func (l *logger) info(format string, args ...interface{}) {
	l.log("\x1b[36m[INFO]\x1b[0m", fmt.Sprintf(format, args...))
}
func (l *logger) error(format string, args ...interface{}) {
	l.log("\x1b[31m[ERROR]\x1b[0m", fmt.Sprintf(format, args...))
}
func (l *logger) warn(format string, args ...interface{}) {
	l.log("\x1b[33m[WARN]\x1b[0m", fmt.Sprintf(format, args...))
}

func (l *logger) log(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Printf("\x1b[90m[%s]\x1b[0m %s %s\n",
		time.Now().Format("15:04:05.000"), level, msg)
}

func (l *logger) order(s *Signal) {
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
		"\x1b[90m[%s]\x1b[0m %sRELAY\x1b[0m %s→%s %s %s %.2f @ %.5f | pnl=%+.2f | %s\n",
		time.Now().Format("15:04:05.000"),
		color,
		action, side,
		"\x1b[37m"+s.Symbol+"\x1b[0m",
		color, s.Lot, s.Price,
		s.Pnl,
		s.Comment)
}
