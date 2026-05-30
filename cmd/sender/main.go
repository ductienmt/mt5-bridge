// Sender: CLI tool gửi signal lên HTTP hoặc TCP bridge
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	flagAction = flag.String("action", "", "OPEN|CLOSE|EDIT (required)")
	flagSide   = flag.String("side", "", "BUY|SELL|BUY_STOP|SELL_STOP|BUY_LIMIT|SELL_LIMIT")
	flagSymbol = flag.String("symbol", "", "Symbol, e.g. EURUSD")
	flagLot    = flag.Float64("lot", 0.01, "Lot size")
	flagPrice  = flag.Float64("price", 0, "Price (for pending orders)")
	flagSL     = flag.Float64("sl", 0, "Stop Loss price")
	flagTP     = flag.Float64("tp", 0, "Take Profit price")
	flagMagic  = flag.Int64("magic", 0, "Magic number")
	flagPnl    = flag.Float64("pnl", 0, "PnL (for CLOSE)")
	flagComment = flag.String("comment", "", "Order comment")
	flagHost   = flag.String("host", "localhost:8080", "Bridge address")
	flagHTTP   = flag.Bool("http", false, "Force HTTP mode")
	flagTCP    = flag.Bool("tcp", false, "Force TCP mode")
	flagBatch  = flag.String("batch", "", "File containing newline-separated JSON signals")
	flagSymbolFile = flag.String("symbol-file", "", "File with symbols (one per line), sends CLOSE to each")
	flagLoop   = flag.Int("loop", 0, "Send signal every N seconds (0=once)")
	flagJSON   = flag.Bool("json", false, "Output full response JSON")
)

type signalPayload struct {
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

type okResp struct {
	Status string `json:"status"`
	Pnl    float64 `json:"pnl"`
	Queue  int    `json:"queue"`
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "MT5 Signal Sender CLI\n\nUsage:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -action OPEN -side BUY -symbol EURUSD -lot 0.1 -sl 1.0800 -tp 1.0900\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -action OPEN -side SELL -symbol EURUSD -lot 0.1 -sl 1.0900 -tp 1.0800\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -action OPEN -side BUY_STOP -symbol EURUSD -lot 0.1 -price 1.0850 -sl 1.0800 -tp 1.0900\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -action CLOSE -symbol EURUSD -pnl -15.50\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -action CLOSE -pnl 50.00\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -action EDIT -symbol EURUSD -sl 1.0750 -tp 1.0950\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -host localhost:8081 -action OPEN -side BUY -symbol XAUUSD -lot 1.0\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -batch signals.txt\n")
		fmt.Fprintf(os.Stderr, "  go run sender.go -action OPEN -side BUY -symbol EURUSD -loop 5\n")
	}
	flag.Parse()

	if *flagAction == "" {
		flag.Usage()
		os.Exit(1)
	}
	if (*flagAction == "OPEN" || *flagAction == "EDIT") && *flagSymbol == "" {
		fmt.Fprintf(os.Stderr, "\n  Error: -symbol is required for OPEN and EDIT\n")
		flag.Usage()
		os.Exit(1)
	}

	payload := signalPayload{
		Action:  strings.ToUpper(*flagAction),
		Side:   strings.ToUpper(*flagSide),
		Symbol: strings.ToUpper(*flagSymbol),
		Lot:    *flagLot,
		Price:  *flagPrice,
		SL:     *flagSL,
		TP:     *flagTP,
		Magic:  *flagMagic,
		Pnl:    *flagPnl,
		Comment: *flagComment,
	}

	transport := detectTransport(*flagHost)
	sendFn := func(p signalPayload) error { return sendSignal(transport, *flagHost, p) }

	if *flagBatch != "" {
		sendBatch(*flagBatch, sendFn)
		return
	}
	if *flagSymbolFile != "" {
		sendSymbolFile(*flagSymbolFile, *flagAction, *flagMagic, *flagPnl, *flagComment, sendFn)
		return
	}
	if *flagLoop > 0 {
		ticker := time.NewTicker(time.Duration(*flagLoop) * time.Second)
		defer ticker.Stop()
		count := 0
		for range ticker.C {
			count++
			if err := sendSignal(transport, *flagHost, payload); err != nil {
				log.Fatalf("Send error: %v", err)
			}
			fmt.Printf("  [loop #%d]\n", count)
		}
		return
	}

	if err := sendSignal(transport, *flagHost, payload); err != nil {
		log.Fatalf("Send error: %v", err)
	}
}

func detectTransport(host string) string {
	if *flagHTTP {
		return "http"
	}
	if *flagTCP {
		return "tcp"
	}
	if strings.Contains(host, ":8081") {
		return "tcp"
	}
	return "http"
}

func sendSignal(transport, host string, p signalPayload) error {
	body, _ := json.Marshal(p)
	if transport == "tcp" {
		return sendTCP(host, body, p)
	}
	return sendHTTP(host, p)
}

func sendHTTP(host string, p signalPayload) error {
	body, _ := json.Marshal(p)
	resp, err := http.Post("http://"+host+"/signal", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("HTTP error: %w", err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		var m map[string]string
		json.Unmarshal(data, &m)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, m["error"])
	}

	if *flagJSON {
		fmt.Println(string(data))
		return nil
	}

	var r okResp
	json.Unmarshal(data, &r)
	fmt.Printf("  \x1b[32mqueued\x1b[0m  %s %s %s lot=%.2f @ %.5f | pnl=%+.2f | queue=%d\n",
		p.Action, p.Side, p.Symbol, p.Lot, p.Price, r.Pnl, r.Queue)
	return nil
}

func sendTCP(host string, body []byte, p signalPayload) error {
	conn, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		return fmt.Errorf("TCP dial: %w", err)
	}
	defer conn.Close()

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write(append(body, '\n'))
	if err != nil {
		return fmt.Errorf("TCP write: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		return fmt.Errorf("TCP read: %w", err)
	}

	if *flagJSON {
		fmt.Println(string(buf[:n]))
		return nil
	}

	var ack map[string]any
	if err := json.Unmarshal(buf[:n], &ack); err != nil {
		fmt.Printf("  \x1b[32mqueued\x1b[0m  (raw ack: %s)\n", string(buf[:n]))
		return nil
	}
	pnl, _ := ack["pnl"].(float64)
	fmt.Printf("  \x1b[32mqueued\x1b[0m  %s %s %s lot=%.2f | pnl=%+.2f | queue=%v\n",
		p.Action, p.Side, p.Symbol, p.Lot, pnl, ack["queue"])
	return nil
}

func sendBatch(path string, sendFn func(signalPayload) error) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Cannot read batch file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var p signalPayload
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			fmt.Fprintf(os.Stderr, "  Line %d: invalid JSON, skipping\n", i+1)
			continue
		}
		if err := sendFn(p); err != nil {
			fmt.Fprintf(os.Stderr, "  Line %d: %v\n", i+1, err)
		}
	}
}

func sendSymbolFile(path, action string, magic int64, pnl float64, comment string, sendFn func(signalPayload) error) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Cannot read symbol file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for i, line := range lines {
		sym := strings.TrimSpace(strings.ToUpper(line))
		if sym == "" || strings.HasPrefix(sym, "#") {
			continue
		}
		p := signalPayload{
			Action:  action,
			Symbol:  sym,
			Magic:   magic,
			Pnl:     pnl,
			Comment: comment,
		}
		if err := sendFn(p); err != nil {
			fmt.Fprintf(os.Stderr, "  Line %d: %v\n", i+1, err)
		}
	}
}
