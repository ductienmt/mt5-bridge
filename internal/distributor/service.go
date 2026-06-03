package distributor

import (
	"context"
	"fmt"

	"mt5-bridge/internal/redis"
	"mt5-bridge/internal/repository"
	"mt5-bridge/signal"
)

// Service provides high-level distribution operations
type Service struct {
	distributor       *SignalDistributor
	subscriptionStore *redis.SubscriptionStore
	followerRepo     *repository.FollowerRepository
	db               interface{ QueryContext(ctx context.Context, query string, args ...interface{}) (interface{ Close(); Next() bool; Scan(dest ...interface{}) error }) }
}

// NewService creates a new distribution service
func NewService(
	distributor *SignalDistributor,
	subscriptionStore *redis.SubscriptionStore,
	followerRepo *repository.FollowerRepository,
) *Service {
	return &Service{
		distributor:       distributor,
		subscriptionStore: subscriptionStore,
		followerRepo:     followerRepo,
	}
}

// DistributeSignal distributes a master signal to all active followers
func (s *Service) DistributeSignal(ctx context.Context, masterID string, masterSignal signal.Signal) (DistributeResult, error) {
	// Get followers from Redis (fast path)
	followerAccountIDs, err := s.subscriptionStore.GetFollowers(ctx, masterID)
	if err != nil {
		return DistributeResult{}, fmt.Errorf("failed to get followers from Redis: %w", err)
	}

	if len(followerAccountIDs) == 0 {
		return DistributeResult{}, nil
	}

	// Get follower details from database
	followers, err := s.followerRepo.GetByAccountIDs(ctx, followerAccountIDs)
	if err != nil {
		return DistributeResult{}, fmt.Errorf("failed to get follower details: %w", err)
	}

	// Distribute to all followers
	return s.distributor.Distribute(masterID, masterSignal, followers), nil
}

// SyncFollowers synchronizes Redis subscriptions from database
func (s *Service) SyncFollowers(ctx context.Context, masterID string) error {
	followers, err := s.followerRepo.GetActiveFollowersByMaster(ctx, masterID)
	if err != nil {
		return fmt.Errorf("failed to get active followers: %w", err)
	}

	// Clear existing and re-add
	if err := s.subscriptionStore.ClearMasterFollowers(ctx, masterID); err != nil {
		return fmt.Errorf("failed to clear followers: %w", err)
	}

	for _, f := range followers {
		if err := s.subscriptionStore.AddFollower(ctx, masterID, f.AccountID); err != nil {
			return fmt.Errorf("failed to add follower: %w", err)
		}
	}

	return nil
}
