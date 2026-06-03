package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// Default settings - optimized for low latency < 1ms
	defaultPoolSize     = 50        // Increased from 20 for better concurrency
	defaultMinIdleConns = 20        // Warm connections for zero-latency first query
	defaultDialTimeout  = 2 * time.Second
	defaultReadTimeout  = 1 * time.Second   // Reduced for faster timeout detection
	defaultWriteTimeout = 1 * time.Second   // Reduced for faster timeout detection
	defaultPoolTimeout  = 500 * time.Millisecond // Connection wait timeout
	defaultReadBufferSize  = 4096  // 4KB read buffer
	defaultWriteBufferSize = 4096  // 4KB write buffer
)

// Config holds Redis configuration
type Config struct {
	Host            string
	Port           int
	Password       string
	DB             int
	PoolSize       int
	MinIdleConns   int
	PoolTimeout    time.Duration
	ReadBufferSize int
	WriteBufferSize int
	UseTLS         bool
}

// NewConfig creates a Config from environment variables
func NewConfig() *Config {
	port := 6379
	if p := os.Getenv("REDIS_PORT"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil {
			port = parsed
		}
	}

	db := 0
	if d := os.Getenv("REDIS_DB"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil {
			db = parsed
		}
	}

	poolSize := defaultPoolSize
	if ps := os.Getenv("REDIS_POOL_SIZE"); ps != "" {
		if parsed, err := strconv.Atoi(ps); err == nil {
			poolSize = parsed
		}
	}

	poolTimeout := defaultPoolTimeout
	if pt := os.Getenv("REDIS_POOL_TIMEOUT"); pt != "" {
		if parsed, err := time.ParseDuration(pt); err == nil {
			poolTimeout = parsed
		}
	}

	readBuffer := defaultReadBufferSize
	if rb := os.Getenv("REDIS_READ_BUFFER"); rb != "" {
		if parsed, err := strconv.Atoi(rb); err == nil {
			readBuffer = parsed
		}
	}

	writeBuffer := defaultWriteBufferSize
	if wb := os.Getenv("REDIS_WRITE_BUFFER"); wb != "" {
		if parsed, err := strconv.Atoi(wb); err == nil {
			writeBuffer = parsed
		}
	}

	return &Config{
		Host:            getEnvOrDefault("REDIS_HOST", "localhost"),
		Port:           port,
		Password:       os.Getenv("REDIS_PASSWORD"),
		DB:             db,
		PoolSize:       poolSize,
		MinIdleConns:   defaultMinIdleConns,
		PoolTimeout:    poolTimeout,
		ReadBufferSize: readBuffer,
		WriteBufferSize: writeBuffer,
		UseTLS:         os.Getenv("REDIS_USE_TLS") == "true",
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Client wraps the Redis client with convenience methods
type Client struct {
	client *redis.Client
}

// Singleton instance
var (
	clientInstance *Client
	clientOnce    struct {
		doOnce func() error
	}
)

// NewClient creates a new Redis client
func NewClient(cfg *Config) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	var tlsConfig *tls.Config
	if cfg.UseTLS {
		tlsConfig = &tls.Config{
			ServerName:         cfg.Host,
			InsecureSkipVerify: false,
		}
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:    cfg.Password,
		DB:          cfg.DB,
		PoolSize:    cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  defaultDialTimeout,
		ReadTimeout:  defaultReadTimeout,
		WriteTimeout: defaultWriteTimeout,
		PoolTimeout:  defaultPoolTimeout,
		TLSConfig:   tlsConfig,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Client{client: rdb}, nil
}

// NewClientFromEnv creates a Redis client from environment variables
func NewClientFromEnv() (*Client, error) {
	return NewClient(NewConfig())
}

// Get returns the global Redis client instance
func Get() *Client {
	return clientInstance
}

// Connect establishes connection to Redis with retry
func Connect(ctx context.Context) (*Client, error) {
	var lastErr error
	maxRetries := 10
	retryInterval := 5 * time.Second

	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		client, err := NewClientFromEnv()
		if err == nil {
			clientInstance = client
			return client, nil
		}

		lastErr = err
		fmt.Printf("Redis connection attempt %d/%d failed: %v\n", i+1, maxRetries, err)
		
		if i < maxRetries-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryInterval):
			}
		}
	}

	return nil, fmt.Errorf("failed to connect to Redis after %d attempts: %w", maxRetries, lastErr)
}

// ConnectWithRetry connects to Redis with infinite retry
func ConnectWithRetry() *Client {
	for {
		client, err := NewClientFromEnv()
		if err == nil {
			fmt.Println("Redis connected successfully")
			clientInstance = client
			return client
		}
		fmt.Printf("Redis unavailable, retrying in 5 seconds: %v\n", err)
		time.Sleep(5 * time.Second)
	}
}

// Ping checks if Redis is reachable
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close closes the Redis connection
func (c *Client) Close() error {
	return c.client.Close()
}

// Underlying returns the underlying redis client
func (c *Client) Underlying() *redis.Client {
	return c.client
}

// SubscribeKeyPrefix is the prefix for master-follower subscription keys
const SubscribeKeyPrefix = "master:followers:"

// BuildSubscribeKey builds a Redis key for master-follower subscriptions
func BuildSubscribeKey(masterID string) string {
	return SubscribeKeyPrefix + masterID
}
