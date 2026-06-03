package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"mt5-bridge/internal/models"
)

const (
	// Hash field limits for optimal performance
	// Using smaller buckets to avoid hash rehash blocking
	BucketCount = 16
)

// FastLookupStore provides O(1) lookups for master-follower mappings
// Uses Redis Hash with bucket sharding for optimal performance
type FastLookupStore struct {
	client *Client
}

// NewFastLookupStore creates a new FastLookupStore
func NewFastLookupStore(client *Client) *FastLookupStore {
	return &FastLookupStore{client: client}
}

// getBucket returns the bucket index for a given key
func getBucket(key string) int {
	h := 0
	for _, c := range key {
		h = 31*h + int(c)
	}
	return h % BucketCount
}

// MasterFollowerInfo stores minimal info needed for signal routing
type MasterFollowerInfo struct {
	MasterID       string  `json:"masterId"`
	AccountID      string  `json:"accountId"`
	Server         string  `json:"server"`
	LotMultiplier  float64 `json:"lotMultiplier"`
	Status         string  `json:"status"`
}

// RegisterMaster registers a master account and publishes registration event
func (s *FastLookupStore) RegisterMaster(ctx context.Context, master *models.Master) error {
	key := fmt.Sprintf("master:%s", master.ID)
	info := MasterFollowerInfo{
		MasterID:  master.ID,
		AccountID: master.AccountID,
		Server:    master.Server,
		Status:    "active",
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal master info: %w", err)
	}

	// Use Set for simple O(1) key-value lookup - faster than Hash
	err = s.client.Underlying().Set(ctx, key, data, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to register master: %w", err)
	}

	// Also add to active masters set for quick enumeration
	err = s.client.Underlying().SAdd(ctx, "masters:active", master.ID).Err()
	if err != nil {
		return fmt.Errorf("failed to add master to active set: %w", err)
	}

	// Publish registration event for any listeners (e.g., TCP workers)
	s.PublishEvent(ctx, "master:registered", master.ID, data)

	return nil
}

// RegisterFollower registers a follower and maps to master with O(1) lookup
func (s *FastLookupStore) RegisterFollower(ctx context.Context, masterID string, follower *models.Follower) error {
	// Store follower info
	followerKey := fmt.Sprintf("follower:%s", follower.ID)
	followerInfo := MasterFollowerInfo{
		MasterID:      masterID,
		AccountID:     follower.AccountID,
		Server:        follower.Server,
		LotMultiplier:  follower.LotMultiplier,
		Status:        follower.Status,
	}

	followerData, err := json.Marshal(followerInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal follower info: %w", err)
	}

	// Use Set for O(1) follower lookup
	err = s.client.Underlying().Set(ctx, followerKey, followerData, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to register follower: %w", err)
	}

	// Create bidirectional mapping for O(1) reverse lookup
	// Key: master -> set of followers
	err = s.client.Underlying().SAdd(ctx, fmt.Sprintf("master:%s:followers", masterID), follower.ID).Err()
	if err != nil {
		return fmt.Errorf("failed to map follower to master: %w", err)
	}

	// Key: follower -> master
	err = s.client.Underlying().Set(ctx, fmt.Sprintf("follower:%s:master", follower.ID), masterID, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to map master to follower: %w", err)
	}

	// Publish registration event
	s.PublishEvent(ctx, "follower:registered", follower.ID, followerData)

	return nil
}

// UnregisterFollower removes a follower from the mapping
func (s *FastLookupStore) UnregisterFollower(ctx context.Context, followerID string) error {
	// Get master ID for this follower
	masterID, err := s.GetMasterForFollower(ctx, followerID)
	if err != nil {
		return fmt.Errorf("failed to get master for follower: %w", err)
	}

	// Remove follower from master's set
	err = s.client.Underlying().SRem(ctx, fmt.Sprintf("master:%s:followers", masterID), followerID).Err()
	if err != nil {
		return fmt.Errorf("failed to remove follower from master set: %w", err)
	}

	// Delete follower-master mapping
	err = s.client.Underlying().Del(ctx, fmt.Sprintf("follower:%s:master", followerID)).Err()
	if err != nil {
		return fmt.Errorf("failed to delete follower-master mapping: %w", err)
	}

	// Delete follower info
	err = s.client.Underlying().Del(ctx, fmt.Sprintf("follower:%s", followerID)).Err()
	if err != nil {
		return fmt.Errorf("failed to delete follower info: %w", err)
	}

	// Publish unregistration event
	s.PublishEvent(ctx, "follower:unregistered", followerID, nil)

	return nil
}

// GetMasterForFollower returns the master ID for a follower - O(1)
func (s *FastLookupStore) GetMasterForFollower(ctx context.Context, followerID string) (string, error) {
	masterID, err := s.client.Underlying().Get(ctx, fmt.Sprintf("follower:%s:master", followerID)).Result()
	if err != nil {
		return "", fmt.Errorf("failed to get master for follower: %w", err)
	}
	return masterID, nil
}

// GetFollowersForMaster returns all follower IDs for a master - O(1) for set size
func (s *FastLookupStore) GetFollowersForMaster(ctx context.Context, masterID string) ([]string, error) {
	followers, err := s.client.Underlying().SMembers(ctx, fmt.Sprintf("master:%s:followers", masterID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get followers for master: %w", err)
	}
	return followers, nil
}

// GetFollowerInfo returns follower info - O(1)
func (s *FastLookupStore) GetFollowerInfo(ctx context.Context, followerID string) (*MasterFollowerInfo, error) {
	data, err := s.client.Underlying().Get(ctx, fmt.Sprintf("follower:%s", followerID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get follower info: %w", err)
	}

	var info MasterFollowerInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal follower info: %w", err)
	}

	return &info, nil
}

// GetMasterInfo returns master info - O(1)
func (s *FastLookupStore) GetMasterInfo(ctx context.Context, masterID string) (*MasterFollowerInfo, error) {
	data, err := s.client.Underlying().Get(ctx, fmt.Sprintf("master:%s", masterID)).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get master info: %w", err)
	}

	var info MasterFollowerInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal master info: %w", err)
	}

	return &info, nil
}

// GetActiveMasters returns all active master IDs - O(n) where n = active masters
func (s *FastLookupStore) GetActiveMasters(ctx context.Context) ([]string, error) {
	masters, err := s.client.Underlying().SMembers(ctx, "masters:active").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get active masters: %w", err)
	}
	return masters, nil
}

// UpdateFollowerStatus updates follower status - O(1)
func (s *FastLookupStore) UpdateFollowerStatus(ctx context.Context, followerID, status string) error {
	info, err := s.GetFollowerInfo(ctx, followerID)
	if err != nil {
		return err
	}

	info.Status = status

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal follower info: %w", err)
	}

	return s.client.Underlying().Set(ctx, fmt.Sprintf("follower:%s", followerID), data, 0).Err()
}

// PublishEvent publishes an event to Redis for event-driven architecture
func (s *FastLookupStore) PublishEvent(ctx context.Context, event, entityID string, data []byte) {
	eventKey := fmt.Sprintf("event:%s:%s", event, entityID)
	if data != nil {
		s.client.Underlying().Publish(ctx, eventKey, data)
	} else {
		s.client.Underlying().Publish(ctx, eventKey, entityID)
	}
}

// SyncFromDatabase syncs all data from database to Redis
// Call this on startup to warm up the cache
func (s *FastLookupStore) SyncFromDatabase(ctx context.Context, masters []*models.Master, followers []*models.Follower) error {
	start := time.Now()
	pipe := s.client.Underlying().Pipeline()

	// Clear existing data
	pipe.Del(ctx, "masters:active")

	for _, master := range masters {
		if master.IsDeleted() {
			continue
		}
		key := fmt.Sprintf("master:%s", master.ID)
		info := MasterFollowerInfo{
			MasterID:  master.ID,
			AccountID: master.AccountID,
			Server:    master.Server,
			Status:    "active",
		}
		data, _ := json.Marshal(info)
		pipe.Set(ctx, key, data, 0)
		pipe.SAdd(ctx, "masters:active", master.ID)
	}

	for _, follower := range followers {
		if follower.IsDeleted() {
			continue
		}

		followerKey := fmt.Sprintf("follower:%s", follower.ID)
		info := MasterFollowerInfo{
			MasterID:     masterIDFromFollowers(followers, follower.MasterID),
			AccountID:    follower.AccountID,
			Server:       follower.Server,
			LotMultiplier: follower.LotMultiplier,
			Status:       follower.Status,
		}
		data, _ := json.Marshal(info)
		pipe.Set(ctx, followerKey, data, 0)
		pipe.SAdd(ctx, fmt.Sprintf("master:%s:followers", follower.MasterID), follower.ID)
		pipe.Set(ctx, fmt.Sprintf("follower:%s:master", follower.ID), follower.MasterID, 0)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync to Redis: %w", err)
	}

	fmt.Printf("Synced %d masters and %d followers to Redis in %v\n", len(masters), len(followers), time.Since(start))
	return nil
}

// Helper to find master ID (simplified - in real code, masters map is passed)
func masterIDFromList(masters []*models.Master, id string) string {
	for _, m := range masters {
		if m.ID == id {
			return m.ID
		}
	}
	return id
}

// masterIDFromFollowers is a helper that just returns the masterID from follower
func masterIDFromFollowers(followers []*models.Follower, masterID string) string {
	return masterID
}
