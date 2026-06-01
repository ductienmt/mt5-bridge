// Relay: nhận signal qua TCP rồi forward lên server HTTP khác
// TCP Bridge → Relay :1082 → http://103.72.56.53:8080
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

// HttpPayload là payload gửi lên FORWARD_TO_URL/signal
// action = BUY/SELL (thứ server cần)
// type = OPEN/CLOSE/EDIT (metadata, không gửi đi)
type HttpPayload struct {
	Action  string  `json:"action"`  // BUY/SELL
	Type    string  `json:"type"`    // OPEN/CLOSE/EDIT
	Symbol  string  `json:"symbol"`
	Lot     float64 `json:"lot"`
	Price   float64 `json:"price"`
	SL      float64 `json:"sl"`
	TP      float64 `json:"tp"`
	Magic   int64   `json:"magic"`
	Pnl     float64 `json:"pnl"`
	Comment string  `json:"comment"`
}

func signalToHttpPayload(s *Signal) *HttpPayload {
	return &HttpPayload{
		Action:  s.Side,   // BUY/SELL — thứ server cần
		// Type:    s.Action,  // OPEN/CLOSE/EDIT — metadata
		Symbol:  s.Symbol,
		Lot:     s.Lot,
		Price:   s.Price,
		SL:      s.SL,
		TP:      s.TP,
		Magic:   s.Magic,
	}
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
	lg.info("Relay listening on %s -> %s/signal", addr, forwardURL)

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

		// Build payload: action=BUY/SELL, type=OPEN/CLOSE/EDIT
		payload := signalToHttpPayload(&sig)

		// Forward lên HTTP server
		respBody, httpStatus, err := forwardSignal(payload)
		latency := time.Since(sentAt)

		if err != nil {
			lg.error("FWD->%s FAIL %s %s %s %.2f | %v",
				forwardURL, payload.Type, payload.Action, payload.Symbol, payload.Lot, err)
			conn.Write([]byte(fmt.Sprintf(`{"ok":false,"error":"%v"}`+"\n", err)))
			continue
		}

		// ── SENT ──────────────────────────────────────────
		lg.sent(forwardURL, payload, httpStatus, latency)
		conn.Write(append(respBody, '\n'))
	}
}

func forwardSignal(payload *HttpPayload) ([]byte, int, error) {
	body, err := json.Marshal(payload)
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

	comment := ""
	if s.Comment != "" {
		comment = " | " + s.Comment
	}

	fmt.Printf(
		"[%s] \x1b[36mRECV\x1b[0m \x1b[90mfrom %s\x1b[0m | %s %s %.2f @ %.5f | SL=%.5f TP=%.5f | magic=%d | pnl=%+.2f%s\n",
		time.Now().Format("15:04:05"),
		from,
		strings.ToUpper(s.Action),
		strings.ToUpper(s.Side),
		s.Lot,
		s.Price,
		s.SL,
		s.TP,
		s.Magic,
		s.Pnl,
		comment,
	)
}

func (l *logger) sent(to string, p *HttpPayload, httpStatus int, latency time.Duration) {
	var color string
	switch strings.ToUpper(p.Type) {
	case "OPEN":
		if strings.HasPrefix(strings.ToUpper(p.Action), "BUY") {
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

	l.mu.Lock()
	defer l.mu.Unlock()

	comment := ""
	if p.Comment != "" {
		comment = " | " + p.Comment
	}

	fmt.Printf(
		"[%s] \x1b[36mSENT\x1b[0m \x1b[90m-> %s\x1b[0m | %s%s %s %.2f @ %.5f | SL=%.5f TP=%.5f | magic=%d | pnl=%+.2f | resp=%d | %s%s\n",
		time.Now().Format("15:04:05"),
		to,
		color, strings.ToUpper(p.Type),
		strings.ToUpper(p.Action),
		p.Lot,
		p.Price,
		p.SL,
		p.TP,
		p.Magic,
		p.Pnl,
		httpStatus,
		latency.Round(time.Millisecond),
		comment,
	)
}
