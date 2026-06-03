// TCP bridge: nhận lệnh từ MT5 EA qua TCP,
// push vào queue (cho DLL đọc) + forward sang relay (để forward đi server khác)
// + distribute signals từ master accounts tới followers
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"mt5-bridge/internal/cache"
	"mt5-bridge/internal/distributor"
	"mt5-bridge/internal/metrics"
	redisclient "mt5-bridge/internal/redis"
	"mt5-bridge/internal/repository"
	sigpkg "mt5-bridge/signal"
)

var (
	q              = sigpkg.Get()
	lg             = sigpkg.NewLogger()
	relayConn      net.Conn
	relayMu        sync.Mutex
	relayHost      string
	relayClose     = make(chan struct{})

	// Master-follower components
	masterCache      *cache.MasterCache
	subStore         *redisclient.SubscriptionStore
	followerRepo     *repository.FollowerRepository
	signalDistributor *distributor.SignalDistributor
)

type Client struct {
	conn     net.Conn
	addr     string
	seen     time.Time
	accountID string // MT5 account ID of the client
}

var (
	clients   = make(map[net.Conn]*Client)
	clientsMu sync.RWMutex
)

func main() {
	sigpkg.Banner()

	// Initialize master-follower components
	initMasterFollower()

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
	lg.Info("Master-Follower system initialized: %d masters in cache", masterCache.Count())

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

// initMasterFollower initializes the master-follower components
func initMasterFollower() {
	// Initialize master cache
	masterCache = cache.Get()

	// Connect to Redis
	redisClient := redisclient.ConnectWithRetry()
	lg.Info("Connected to Redis for master-follower subscriptions")

	subStore = redisclient.NewSubscriptionStore(redisClient)

	// Connect to database
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://admin:admin123@localhost:1090/mt5_bridge"
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		lg.Warn("Failed to open database: %v — master-follower disabled", err)
		return
	}
	if err := db.Ping(); err != nil {
		lg.Warn("Failed to ping database: %v — master-follower disabled", err)
		db.Close()
		return
	}
	lg.Info("Connected to database for master-follower")

	// Load masters into cache
	followerRepo = repository.NewFollowerRepository(db)
	if err := masterCache.LoadFromDB(db); err != nil {
		lg.Warn("Failed to load master cache: %v", err)
	} else {
		lg.Info("Loaded %d masters into cache", masterCache.Count())
	}

	// Initialize signal distributor
	signalDistributor = distributor.NewSignalDistributor(10)
	go followerOrderWorker()

	// Update metrics
	metrics.UpdateActiveMasters(masterCache.Count())
}

// followerOrderWorker processes follower orders from the distributor channel
func followerOrderWorker() {
	for order := range signalDistributor.GetQueueChannel() {
		// In a real implementation, this would send the order to the follower's MT5 EA
		// For now, we just log it
		lg.Info("FOLLOWER ORDER: %s -> %s %s %.2f @ %.5f",
			order.Signal.AccountID,
			order.FollowerAccountID,
			strings.ToUpper(order.Signal.Side),
			order.Signal.Lot,
			order.Signal.Price,
		)

		// Here you would:
		// 1. Establish connection to follower's MT5 EA (or send via relay)
		// 2. Push the signal to follower's queue
		// 3. Record metrics
	}
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

		// Tắt Nagle's Algorithm để gói tin được gửi ngay lập tức
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true)
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

// isMasterSignal checks if a signal is from a registered master account
func isMasterSignal(accountID string) (string, bool) {
	if masterCache == nil {
		return "", false
	}
	return masterCache.Get(accountID)
}

// distributeToFollowers distributes a master signal to all active followers
func distributeToFollowers(masterID string, sig sigpkg.Signal) {
	if subStore == nil || followerRepo == nil || signalDistributor == nil {
		return
	}

	startTime := time.Now()

	// Get followers from Redis
	ctx := context.Background()
	followerAccountIDs, err := subStore.GetFollowers(ctx, masterID)
	if err != nil {
		lg.Error("Failed to get followers from Redis: %v", err)
		metrics.RecordRedisError("SMEMBERS")
		return
	}

	if len(followerAccountIDs) == 0 {
		lg.Info("Master %s has no active followers", masterID)
		return
	}

	// Get follower details from database
	followers, err := followerRepo.GetByAccountIDs(ctx, followerAccountIDs)
	if err != nil {
		lg.Error("Failed to get follower details: %v", err)
		metrics.RecordDatabaseError("SELECT")
		return
	}

	if len(followers) == 0 {
		return
	}

	// Distribute to all followers
	result := signalDistributor.Distribute(masterID, sig, followers)

	latencyMs := time.Since(startTime).Milliseconds()
	metrics.RecordDistributionSuccess(masterID, result.SuccessCount, float64(latencyMs))

	lg.Info("DISTRIBUTED: master=%s signal=%s/%s %.2f -> %d followers (%.2fms)",
		masterID, sig.Action, sig.Side, sig.Lot, result.SuccessCount, float64(latencyMs)/1000)

	if result.ErrorCount > 0 {
		lg.Warn("Distribution had %d errors for master %s", result.ErrorCount, masterID)
	}

	if latencyMs > 100 {
		lg.Warn("Distribution latency exceeded 100ms: %dms", latencyMs)
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
			processSignal(conn, addr, sig, line)
			continue
		}

		sig, err := sigpkg.ParseSignal(line)
		if err != nil {
			lg.Warn("Parse error from %s: %v", addr, err)
			continue
		}

		processSignal(conn, addr, sig, line)
	}

	if err := scanner.Err(); err != nil {
		if !strings.Contains(err.Error(), "use of closed network connection") && err.Error() != "EOF" {
			lg.Error("Connection error [%s]: %v", addr, err)
		}
	}

	lg.Info("MT5 EA disconnected: %s", addr)
}

// processSignal handles a parsed signal - push to queue and check for master-follower
// Được tối ưu để:
// 1. Gửi ACK ngay lập tức (không cần lookup connection)
// 2. Forward sang relay bất đồng bộ (goroutine riêng)
// 3. Xử lý master-follower bất đồng bộ (không block hot path)
func processSignal(conn net.Conn, addr string, sig sigpkg.Signal, rawLine []byte) {
	// Push to queue for local processing
	q.Push(sig)
	tcpLog("RECV", addr, &sig)

	// Gửi ACK ngay lập tức - không cần lookup connection nữa
	ack := map[string]any{
		"status": "queued",
		"queue":  q.Size(),
		"pnl":    sig.Pnl,
		"time":   time.Now().Format(time.RFC3339),
	}
	ackBytes, _ := json.Marshal(ack)
	if conn != nil {
		conn.Write(append(ackBytes, '\n'))
	}

	// 1. Forward sang relay BẤT ĐỒNG BỘ - không block hot path
	if relayHost != "" {
		go func(data []byte) {
			sendToRelay(data)
			tcpLog("FWD ", addr, &sig)
		}(rawLine)
	}

	// 2. Xử lý Master-Follower BẤT ĐỒNG BỘ - Redis/DB I/O không block forward
	if sig.AccountID != "" {
		go func(s sigpkg.Signal) {
			masterID, isMaster := isMasterSignal(s.AccountID)
			if isMaster {
				metrics.SignalsReceived.WithLabelValues(masterID).Inc()
				distributeToFollowers(masterID, s)
			}
		}(sig)
	}
}

// getConnByAddr finds a client connection by address
func getConnByAddr(addr string) (net.Conn, bool) {
	clientsMu.RLock()
	defer clientsMu.RUnlock()
	for conn, client := range clients {
		if client.addr == addr {
			return conn, true
		}
	}
	return nil, false
}

func parsePipe(raw string) (sigpkg.Signal, bool) {
	parts := strings.Split(strings.TrimSpace(raw), "|")
	if len(parts) < 2 {
		return sigpkg.Signal{}, false
	}
	sig := sigpkg.Signal{Action: parts[0], Side: parts[1], Symbol: parts[2], Time: time.Now()}
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
