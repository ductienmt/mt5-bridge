package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"mt5-bridge/internal/models"
)

// SubscriptionStore manages master-follower subscriptions in Redis
type SubscriptionStore struct {
	client *Client
}

// NewSubscriptionStore creates a new SubscriptionStore
func NewSubscriptionStore(client *Client) *SubscriptionStore {
	return &SubscriptionStore{client: client}
}

// AddFollower adds a follower to a master's subscription set
func (s *SubscriptionStore) AddFollower(ctx context.Context, masterID, followerAccountID string) error {
	key := BuildSubscribeKey(masterID)
	err := s.client.Underlying().SAdd(ctx, key, followerAccountID).Err()
	if err != nil {
		return fmt.Errorf("failed to add follower to Redis: %w", err)
	}
	return nil
}

// RemoveFollower removes a follower from a master's subscription set
func (s *SubscriptionStore) RemoveFollower(ctx context.Context, masterID, followerAccountID string) error {
	key := BuildSubscribeKey(masterID)
	err := s.client.Underlying().SRem(ctx, key, followerAccountID).Err()
	if err != nil {
		return fmt.Errorf("failed to remove follower from Redis: %w", err)
	}
	return nil
}

// GetFollowers returns all follower account IDs for a master
func (s *SubscriptionStore) GetFollowers(ctx context.Context, masterID string) ([]string, error) {
	key := BuildSubscribeKey(masterID)
	followers, err := s.client.Underlying().SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get followers from Redis: %w", err)
	}
	return followers, nil
}

// GetFollowerCount returns the number of active followers for a master
func (s *SubscriptionStore) GetFollowerCount(ctx context.Context, masterID string) (int64, error) {
	key := BuildSubscribeKey(masterID)
	count, err := s.client.Underlying().SCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get follower count from Redis: %w", err)
	}
	return count, nil
}

// IsMember checks if an account ID is a follower of a master
func (s *SubscriptionStore) IsMember(ctx context.Context, masterID, followerAccountID string) (bool, error) {
	key := BuildSubscribeKey(masterID)
	isMember, err := s.client.Underlying().SIsMember(ctx, key, followerAccountID).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check membership in Redis: %w", err)
	}
	return isMember, nil
}

// ClearMasterFollowers removes all followers for a master
func (s *SubscriptionStore) ClearMasterFollowers(ctx context.Context, masterID string) error {
	key := BuildSubscribeKey(masterID)
	err := s.client.Underlying().Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to clear master followers from Redis: %w", err)
	}
	return nil
}

// SyncFromFollowers synchronizes Redis subscriptions from a list of followers
func (s *SubscriptionStore) SyncFromFollowers(ctx context.Context, followers []*models.Follower) error {
	// Group followers by master ID
	masterFollowers := make(map[string][]string)
	for _, f := range followers {
		masterFollowers[f.MasterID] = append(masterFollowers[f.MasterID], f.AccountID)
	}

	pipe := s.client.Underlying().Pipeline()

	for masterID, followerList := range masterFollowers {
		key := BuildSubscribeKey(masterID)
		// Delete existing key
		pipe.Del(ctx, key)
		// Add all active followers
		if len(followerList) > 0 {
			args := make([]interface{}, len(followerList))
			for i, f := range followerList {
				args[i] = f
			}
			pipe.SAdd(ctx, key, args...)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync followers to Redis: %w", err)
	}

	return nil
}

// SyncFromFollowerList synchronizes Redis subscriptions from a list of follower info
func (s *SubscriptionStore) SyncFromFollowerList(ctx context.Context, masterFollowers map[string][]string) error {
	pipe := s.client.Underlying().Pipeline()

	for masterID, followerList := range masterFollowers {
		key := BuildSubscribeKey(masterID)
		// Delete existing key
		pipe.Del(ctx, key)
		// Add all active followers
		if len(followerList) > 0 {
			args := make([]interface{}, len(followerList))
			for i, f := range followerList {
				args[i] = f
			}
			pipe.SAdd(ctx, key, args...)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to sync followers to Redis: %w", err)
	}

	return nil
}

// HealthCheck performs a health check on Redis
func (s *SubscriptionStore) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.client.Ping(ctx)
}

// GetAllMasterKeys returns all master subscription keys
func (s *SubscriptionStore) GetAllMasterKeys(ctx context.Context) ([]string, error) {
	pattern := SubscribeKeyPrefix + "*"
	keys, err := s.client.Underlying().Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get master keys: %w", err)
	}
	// Strip prefix for cleaner output
	result := make([]string, len(keys))
	for i, key := range keys {
		result[i] = strings.TrimPrefix(key, SubscribeKeyPrefix)
	}
	return result, nil
}
