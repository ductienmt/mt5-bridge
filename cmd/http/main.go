// HTTP bridge: Go server nhận HTTP POST từ bên ngoài
// rồi đẩy vào shared queue → MT5 EA đọc qua DLL
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	sigpkg "mt5-bridge/signal"
)

var q  = sigpkg.Get()
var lg = sigpkg.NewLogger()

type ErrorResp struct {
	Error string `json:"error"`
}

type OKResp struct {
	Status string  `json:"status"`
	Pnl    float64 `json:"pnl"`
	Queue  int     `json:"queue"`
}

func handleSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResp{Error: "Only POST allowed"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 32*1024))
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ErrorResp{Error: "Failed to read body"})
		return
	}

	sig, err := sigpkg.ParseSignal(body)
	if err != nil {
		lg.Warn("Parse error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ErrorResp{Error: err.Error()})
		return
	}

	q.Push(sig)
	lg.Order(&sig)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(OKResp{Status: "queued", Pnl: sig.Pnl, Queue: q.Size()})
}

func handleQueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		json.NewEncoder(w).Encode(map[string]any{"size": q.Size()})
	case http.MethodDelete:
		q.Clear()
		lg.Info("Queue cleared")
		json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResp{Error: "GET or DELETE only"})
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":  "running",
		"queue":   q.Size(),
		"version": "1.03",
	})
}

func handleRaw(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4*1024))
	parts := strings.Split(strings.TrimSpace(string(body)), "|")
	if len(parts) < 2 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Usage: action|symbol|lot|price|sl|tp|magic|comment")
		return
	}

	sig := sigpkg.Signal{Action: parts[0], Time: time.Now()}
	if len(parts) > 1 { sig.Symbol = parts[1] }
	if len(parts) > 2 { fmt.Sscanf(parts[2], "%f", &sig.Lot) }
	if len(parts) > 3 { fmt.Sscanf(parts[3], "%f", &sig.Price) }
	if len(parts) > 4 { fmt.Sscanf(parts[4], "%f", &sig.SL) }
	if len(parts) > 5 { fmt.Sscanf(parts[5], "%f", &sig.TP) }
	if len(parts) > 6 { fmt.Sscanf(parts[6], "%d", &sig.Magic) }
	if len(parts) > 7 { sig.Comment = parts[7] }

	q.Push(sig)
	lg.Order(&sig)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(OKResp{Status: "queued", Pnl: sig.Pnl, Queue: q.Size()})
}

func main() {
	sigpkg.Banner()

	port := getEnv("MT5_HTTP_PORT", "8080")

	mux := http.NewServeMux()
	mux.HandleFunc("/signal", handleSignal)
	mux.HandleFunc("/queue", handleQueue)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/raw", handleRaw)

	addr := ":" + port

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		lg.Info("Shutting down HTTP server...")
		os.Exit(0)
	}()

	lg.Info("HTTP server listening on %s", addr)
	lg.Info("POST /signal  — queue a trading signal (JSON)")
	lg.Info("POST /raw     — queue a trading signal (pipe-separated text)")
	lg.Info("GET  /queue   — get queue size")
	lg.Info("DELETE /queue — clear queue")
	lg.Info("GET  /health  — health check")

	if err := http.ListenAndServe(addr, mux); err != nil {
		lg.Error("Server error: %v", err)
		log.Fatal(err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
