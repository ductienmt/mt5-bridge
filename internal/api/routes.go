package api

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	"mt5-bridge/internal/cache"
	"mt5-bridge/internal/metrics"
	"mt5-bridge/internal/redis"
)

// SetupRoutes configures the HTTP routes
func SetupRoutes(mux *http.ServeMux, server *Server) {
	// Auth endpoints (no auth required)
	mux.HandleFunc("/api/auth/login", server.Login)

	// Master endpoints
	mux.HandleFunc("/api/masters", server.CreateMaster)
	mux.HandleFunc("/api/masters/", func(w http.ResponseWriter, r *http.Request) {
		// Route based on method and path
		path := r.URL.Path

		// POST /api/masters/{masterId}/followers
		if r.Method == http.MethodPost && containsSubstring(path, "/followers") {
			server.RegisterFollower(w, r)
			return
		}

		// GET /api/masters/{masterId}/followers
		if r.Method == http.MethodGet && containsSubstring(path, "/followers") {
			server.GetMasterFollowers(w, r)
			return
		}

		// DELETE /api/masters/{masterId}
		if r.Method == http.MethodDelete {
			server.DeleteMaster(w, r)
			return
		}

		http.NotFound(w, r)
	})

	// Follower endpoints
	mux.HandleFunc("/api/followers/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// POST /api/followers/{followerId}/start
		if r.Method == http.MethodPost && containsSubstring(path, "/start") {
			server.StartFollowing(w, r)
			return
		}

		// POST /api/followers/{followerId}/stop
		if r.Method == http.MethodPost && containsSubstring(path, "/stop") {
			server.StopFollowing(w, r)
			return
		}

		// DELETE /api/followers/{followerId}
		if r.Method == http.MethodDelete {
			server.DeleteFollower(w, r)
			return
		}

		http.NotFound(w, r)
	})

	// Health check
	mux.HandleFunc("/health", server.HealthCheck)

	// Metrics endpoint (Prometheus)
	mux.Handle("/metrics", metrics.Handler())
}

// containsSubstring checks if a path contains a substring
func containsSubstring(path, substr string) bool {
	for i := 0; i <= len(path)-len(substr); i++ {
		if path[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// StartServer starts the API server
func StartServer(db *sql.DB, port int) error {
	// Initialize components
	masterCache := cache.Get()

	// Connect to Redis
	redisClient := redis.ConnectWithRetry()

	subStore := redis.NewSubscriptionStore(redisClient)
	fastLookup := redis.NewFastLookupStore(redisClient)

	// Create server
	server := NewServer(db, masterCache, subStore, fastLookup)

	// Load master cache from database
	if err := masterCache.LoadFromDB(db); err != nil {
		log.Printf("Warning: Failed to load master cache: %v", err)
	}

	// Create router
	mux := http.NewServeMux()

	// Apply middleware
	var handler http.Handler = mux
	handler = RecoveryMiddleware(handler)
	handler = LoggingMiddleware(handler)
	handler = CORSMiddleware(handler)
	handler = AuthMiddleware(handler)
	handler = ValidateContentType(handler)

	// Setup routes
	SetupRoutes(mux, server)

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting API server on %s", addr)
	return http.ListenAndServe(addr, handler)
}
