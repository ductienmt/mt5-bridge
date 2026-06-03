package cache

import (
	"sync"
	"testing"
)

func TestMasterCache_Get_Set(t *testing.T) {
	cache := &MasterCache{
		cache: make(map[string]string),
	}

	// Test Set and Get
	cache.Set("account123", "master456")

	masterID, found := cache.Get("account123")
	if !found {
		t.Error("Expected to find account123 in cache")
	}
	if masterID != "master456" {
		t.Errorf("Expected masterID 'master456', got '%s'", masterID)
	}
}

func TestMasterCache_Get_NotFound(t *testing.T) {
	cache := &MasterCache{
		cache: make(map[string]string),
	}

	_, found := cache.Get("nonexistent")
	if found {
		t.Error("Expected not to find nonexistent account")
	}
}

func TestMasterCache_Delete(t *testing.T) {
	cache := &MasterCache{
		cache: make(map[string]string),
	}

	cache.Set("account123", "master456")
	cache.Delete("account123")

	_, found := cache.Get("account123")
	if found {
		t.Error("Expected account123 to be deleted")
	}
}

func TestMasterCache_Clear(t *testing.T) {
	cache := &MasterCache{
		cache: make(map[string]string),
	}

	cache.Set("account1", "master1")
	cache.Set("account2", "master2")
	cache.Set("account3", "master3")

	if cache.Size() != 3 {
		t.Errorf("Expected size 3, got %d", cache.Size())
	}

	cache.Clear()

	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", cache.Size())
	}
}

func TestMasterCache_Size(t *testing.T) {
	cache := &MasterCache{
		cache: make(map[string]string),
	}

	if cache.Size() != 0 {
		t.Errorf("Expected size 0, got %d", cache.Size())
	}

	cache.Set("account1", "master1")
	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}

	cache.Set("account2", "master2")
	if cache.Size() != 2 {
		t.Errorf("Expected size 2, got %d", cache.Size())
	}
}

func TestMasterCache_ConcurrentAccess(t *testing.T) {
	cache := &MasterCache{
		cache: make(map[string]string),
	}

	var wg sync.WaitGroup
	numGoroutines := 100
	numOperations := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				cache.Set(string(rune('a'+id%26))+string(rune('0'+j%10)), "master")
			}
		}(i)
	}

	wg.Wait()

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := string(rune('a'+id%26)) + string(rune('0'+j%10))
				_, _ = cache.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// No panic means success
}

func TestMasterCache_Count(t *testing.T) {
	cache := &MasterCache{
		cache: make(map[string]string),
	}

	cache.Set("account1", "master1")
	cache.Set("account2", "master2")

	count := cache.Count()
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}
}

func TestMasterCache_GetAll(t *testing.T) {
	cache := &MasterCache{
		cache: make(map[string]string),
	}

	cache.Set("account1", "master1")
	cache.Set("account2", "master2")

	all := cache.GetAll()
	if len(all) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(all))
	}

	if all["account1"] != "master1" {
		t.Error("Expected account1 -> master1")
	}
	if all["account2"] != "master2" {
		t.Error("Expected account2 -> master2")
	}
}

func TestGetSingleton(t *testing.T) {
	// Reset singleton for testing
	instance = nil
	once = sync.Once{}

	cache1 := Get()
	cache2 := Get()

	if cache1 != cache2 {
		t.Error("Get() should return the same singleton instance")
	}
}
