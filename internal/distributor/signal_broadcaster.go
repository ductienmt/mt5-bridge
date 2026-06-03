package distributor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"mt5-bridge/internal/redis"
	"mt5-bridge/signal"
)

// SignalBroadcaster handles low-latency signal distribution to followers
// Optimized for sub-millisecond latency using Redis pub/sub
type SignalBroadcaster struct {
	signalHub   *redis.SignalHub
	lookup     *redis.FastLookupStore
	subscriptions map[string]chan *signal.Signal // workerID -> signal channel
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewSignalBroadcaster creates a new SignalBroadcaster
func NewSignalBroadcaster(redisClient *redis.Client) *SignalBroadcaster {
	ctx, cancel := context.WithCancel(context.Background())
	return &SignalBroadcaster{
		signalHub:   redis.NewSignalHub(redisClient),
		lookup:      redis.NewFastLookupStore(redisClient),
		subscriptions: make(map[string]chan *signal.Signal),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// StartFollower starts forwarding signals to a follower
// This is called when a follower starts copy trading
func (b *SignalBroadcaster) StartFollower(workerID, masterID string) error {
	// Subscribe to master's signal channel
	signalCh, err := b.signalHub.SubscribeToMaster(b.ctx, masterID, workerID)
	if err != nil {
		return fmt.Errorf("failed to subscribe to master: %w", err)
	}

	b.mu.Lock()
	// Convert read-only channel to buffered channel for storage
	bufCh := make(chan *signal.Signal, 100)
	b.subscriptions[workerID] = bufCh
	b.mu.Unlock()

	// Forward signals from subscription to buffered channel
	go func() {
		for sig := range signalCh {
			select {
			case bufCh <- sig:
			case <-b.ctx.Done():
				return
			}
		}
		close(bufCh)
	}()

	log.Printf("Worker %s subscribed to master %s signals", workerID, masterID)
	return nil
}

// StopFollower stops forwarding signals to a follower
func (b *SignalBroadcaster) StopFollower(workerID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.subscriptions[workerID]; ok {
		close(ch)
		delete(b.subscriptions, workerID)
	}

	b.signalHub.UnsubscribeFromMaster(workerID)
}

// BroadcastToMaster publishes a signal from a master to all its followers
// This is the main entry point for signal distribution
func (b *SignalBroadcaster) BroadcastToMaster(ctx context.Context, masterID string, sig *signal.Signal) error {
	start := time.Now()
	
	// Publish to Redis channel - this is O(1) and typically takes < 0.1ms
	if err := b.signalHub.PublishSignal(ctx, masterID, sig); err != nil {
		return fmt.Errorf("failed to broadcast signal: %w", err)
	}

	latency := time.Since(start)
	if latency > 500*time.Microsecond {
		log.Printf("WARNING: Signal broadcast latency: %v (target: <1ms)", latency)
	}

	return nil
}

// BroadcastToFollowers publishes signals directly to multiple followers
// Use this for batch processing when you have multiple followers
func (b *SignalBroadcaster) BroadcastToFollowers(ctx context.Context, followerIDs []string, sig *signal.Signal) error {
	if len(followerIDs) == 0 {
		return nil
	}

	start := time.Now()

	// Get master info for each follower and group by master
	masterFollowers := make(map[string][]string)
	for _, fid := range followerIDs {
		info, err := b.lookup.GetFollowerInfo(ctx, fid)
		if err != nil {
			log.Printf("Failed to get follower info for %s: %v", fid, err)
			continue
		}
		if info.Status != "active" {
			continue
		}
		masterFollowers[info.MasterID] = append(masterFollowers[info.MasterID], fid)
	}

	// Publish to each master's channel
	for masterID := range masterFollowers {
		if err := b.signalHub.PublishSignal(ctx, masterID, sig); err != nil {
			log.Printf("Failed to publish to master %s: %v", masterID, err)
		}
	}

	latency := time.Since(start)
	if latency > 1*time.Millisecond {
		log.Printf("WARNING: Batch broadcast latency: %v", latency)
	}

	return nil
}

// GetSignalChannel returns the signal channel for a worker
func (b *SignalBroadcaster) GetSignalChannel(workerID string) (<-chan *signal.Signal, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ch, ok := b.subscriptions[workerID]
	return ch, ok
}

// GetActiveWorkerCount returns the number of active workers
func (b *SignalBroadcaster) GetActiveWorkerCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscriptions)
}

// Close shuts down the broadcaster
func (b *SignalBroadcaster) Close() {
	b.cancel()
	b.mu.Lock()
	defer b.mu.Unlock()
	for workerID, ch := range b.subscriptions {
		close(ch)
		b.signalHub.UnsubscribeFromMaster(workerID)
	}
	b.subscriptions = make(map[string]chan *signal.Signal)
}

// LookupFollower looks up follower info - O(1)
func (b *SignalBroadcaster) LookupFollower(ctx context.Context, followerID string) (*redis.MasterFollowerInfo, error) {
	return b.lookup.GetFollowerInfo(ctx, followerID)
}

// LookupMaster looks up master info - O(1)
func (b *SignalBroadcaster) LookupMaster(ctx context.Context, masterID string) (*redis.MasterFollowerInfo, error) {
	return b.lookup.GetMasterInfo(ctx, masterID)
}

// GetFollowersForMaster returns all follower IDs for a master
func (b *SignalBroadcaster) GetFollowersForMaster(ctx context.Context, masterID string) ([]string, error) {
	return b.lookup.GetFollowersForMaster(ctx, masterID)
}

// DistributeSignal is the main function called by the TCP worker
// It looks up which master this account belongs to and broadcasts the signal
func (b *SignalBroadcaster) DistributeSignal(ctx context.Context, accountID string, sig *signal.Signal) error {
	// First, find which master this account belongs to
	masterID, err := b.lookup.GetMasterForFollower(ctx, accountID)
	if err != nil {
		return fmt.Errorf("account %s is not a follower: %w", accountID, err)
	}

	// Apply lot multiplier based on follower settings
	info, err := b.lookup.GetFollowerInfo(ctx, accountID)
	if err != nil {
		return fmt.Errorf("failed to get follower info: %w", err)
	}

	// Scale the signal - apply lot multiplier
	scaledSig := *sig
	scaledSig.Lot = sig.Lot * info.LotMultiplier

	// Broadcast to the master channel
	return b.BroadcastToMaster(ctx, masterID, &scaledSig)
}

// SignalMessage is the message format for Redis pub/sub
type SignalMessage struct {
	MasterID     string          `json:"masterId"`
	Signal       json.RawMessage `json:"signal"`
	Timestamp    time.Time       `json:"timestamp"`
	FollowerIDs  []string        `json:"followerIds,omitempty"`
}
