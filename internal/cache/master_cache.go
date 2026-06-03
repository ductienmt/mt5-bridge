package cache

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/lib/pq"
)

// MasterCache provides O(1) lookup for master accounts
type MasterCache struct {
	mu    sync.RWMutex
	cache map[string]string // accountId -> masterId
}

// Singleton instance
var (
	instance *MasterCache
	once     sync.Once
)

// Get returns the singleton MasterCache instance
func Get() *MasterCache {
	once.Do(func() {
		instance = &MasterCache{
			cache: make(map[string]string),
		}
	})
	return instance
}

// Get looks up a master by accountId
// Returns (masterId, isMaster)
func (mc *MasterCache) Get(accountId string) (string, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	masterId, found := mc.cache[accountId]
	return masterId, found
}

// Set adds or updates a master in the cache
func (mc *MasterCache) Set(accountId, masterId string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.cache[accountId] = masterId
}

// Delete removes a master from the cache
func (mc *MasterCache) Delete(accountId string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.cache, accountId)
}

// Clear removes all entries from the cache
func (mc *MasterCache) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.cache = make(map[string]string)
}

// Size returns the number of entries in the cache
func (mc *MasterCache) Size() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return len(mc.cache)
}

// LoadFromDB loads all non-deleted masters from the database into the cache
func (mc *MasterCache) LoadFromDB(db *sql.DB) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	query := `
		SELECT id, account_id 
		FROM masters 
		WHERE deleted_at IS NULL
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to load masters from database: %w", err)
	}
	defer rows.Close()

	mc.cache = make(map[string]string)
	for rows.Next() {
		var id, accountId string
		if err := rows.Scan(&id, &accountId); err != nil {
			return fmt.Errorf("failed to scan master row: %w", err)
		}
		mc.cache[accountId] = id
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating master rows: %w", err)
	}

	return nil
}

// Count returns the count of cached masters
func (mc *MasterCache) Count() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return len(mc.cache)
}

// GetAll returns all cached accountId -> masterId mappings
func (mc *MasterCache) GetAll() map[string]string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	result := make(map[string]string, len(mc.cache))
	for k, v := range mc.cache {
		result[k] = v
	}
	return result
}
