package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"

	"mt5-bridge/internal/api"
	"mt5-bridge/internal/cache"
	"mt5-bridge/internal/redis"
	"mt5-bridge/internal/repository"

	_ "mt5-bridge/internal/models" // import models for init
)

const (
	defaultPort = 8082
	version    = "2.0.0"
)

func main() {
	log.Printf("Starting MT5 Master-Follower API Server v%s", version)

	// Load configuration from environment
	dbURL := getEnvOrDefault("DATABASE_URL", "postgres://admin:admin123@localhost:1090/mt5_bridge")
	port := getEnvOrInt("API_PORT", defaultPort)

	// Connect to database
	db, err := connectDatabase(dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	log.Println("Connected to PostgreSQL database")

	// Connect to Redis
	redisClient := redis.ConnectWithRetry()
	defer redisClient.Close()

	log.Println("Connected to Redis")

	// Initialize master cache and load from database
	masterCache := cache.Get()
	if err := masterCache.LoadFromDB(db); err != nil {
		log.Printf("Warning: Failed to load master cache from database: %v", err)
	} else {
		log.Printf("Loaded %d masters into cache", masterCache.Count())
	}

	// Sync Redis subscriptions from database
	subStore := redis.NewSubscriptionStore(redisClient)
	followerRepo := repository.NewFollowerRepository(db)

	// Sync followers from DB to Redis
	if followers, err := followerRepo.GetAll(context.Background()); err == nil {
		masterFollowers := make(map[string][]string)
		for _, f := range followers {
			masterFollowers[f.MasterID] = append(masterFollowers[f.MasterID], f.AccountID)
		}
		if err := subStore.SyncFromFollowerList(context.Background(), masterFollowers); err != nil {
			log.Printf("Warning: Failed to sync followers to Redis: %v", err)
		}
	}

	// Start API server
	log.Printf("Starting API server on port %d", port)
	if err := api.StartServer(db, port); err != nil {
		log.Fatalf("Failed to start API server: %v", err)
	}
}

// connectDatabase connects to the PostgreSQL database with retry
func connectDatabase(dbURL string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	// Retry connection
	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", dbURL)
		if err == nil {
			err = db.Ping()
		}
		if err == nil {
			// Configure connection pool
			db.SetMaxOpenConns(50)
			db.SetMaxIdleConns(5)
			db.SetConnMaxLifetime(5 * time.Minute)
			return db, nil
		}

		log.Printf("Database connection attempt %d failed: %v", i+1, err)
		time.Sleep(5 * time.Second)
	}

	return nil, fmt.Errorf("failed to connect to database after 10 attempts: %w", err)
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
