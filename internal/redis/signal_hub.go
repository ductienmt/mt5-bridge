package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"mt5-bridge/signal"
)

const (
	// Channel key prefixes
	SignalChannelPrefix = "signal:channel:"

	// Hash keys for fast O(1) lookups
	MasterFollowerHash    = "map:master:followers"
	FollowerMasterHash    = "map:follower:master"
	MasterInfoHash        = "map:master:info:"
	FollowerInfoHash      = "map:follower:info:"

	// PubSub buffer settings for low latency
	PubSubBufSize = 4096
)

// SignalHub handles pub/sub for trading signals with sub-millisecond latency
type SignalHub struct {
	client       *Client
	subscriptions map[string]*Client // Per-worker subscription clients
	mu           sync.RWMutex
}

// NewSignalHub creates a new SignalHub
func NewSignalHub(client *Client) *SignalHub {
	return &SignalHub{
		client:       client,
		subscriptions: make(map[string]*Client),
	}
}

// BuildChannelKey builds a Redis channel key for a master
func BuildChannelKey(masterID string) string {
	return SignalChannelPrefix + masterID
}

// PublishSignal publishes a trading signal to all followers of a master
// This is called when a master opens/closes/modifies a trade
func (h *SignalHub) PublishSignal(ctx context.Context, masterID string, sig *signal.Signal) error {
	channel := BuildChannelKey(masterID)

	data, err := json.Marshal(sig)
	if err != nil {
		return fmt.Errorf("failed to marshal signal: %w", err)
	}

	// Publish to Redis channel - O(1) operation, typically < 0.1ms
	err = h.client.Underlying().Publish(ctx, channel, data).Err()
	if err != nil {
		return fmt.Errorf("failed to publish signal: %w", err)
	}

	return nil
}

// SubscribeToMaster subscribes to a master's signal channel
// Returns a channel that receives signals
func (h *SignalHub) SubscribeToMaster(ctx context.Context, masterID, workerID string) (<-chan *signal.Signal, error) {
	channel := BuildChannelKey(masterID)

	// Create a dedicated client for this subscription to avoid blocking
	subClient, err := NewClientFromEnv()
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription client: %w", err)
	}

	h.mu.Lock()
	h.subscriptions[workerID] = subClient
	h.mu.Unlock()

	// Subscribe to channel
	pubsub := subClient.Underlying().Subscribe(ctx, channel)

	// Create signal channel
	signalCh := make(chan *signal.Signal, PubSubBufSize)

	// Start goroutine to forward messages
	go func() {
		defer close(signalCh)
		defer pubsub.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-pubsub.Channel():
				if msg == nil {
					return
				}
				var sig signal.Signal
				if err := json.Unmarshal([]byte(msg.Payload), &sig); err != nil {
					continue
				}
				select {
				case signalCh <- &sig:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return signalCh, nil
}

// UnsubscribeFromMaster unsubscribes from a master's signal channel
func (h *SignalHub) UnsubscribeFromMaster(workerID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if client, ok := h.subscriptions[workerID]; ok {
		client.Close()
		delete(h.subscriptions, workerID)
	}
}

// GetSubscriptionCount returns the number of active subscriptions
func (h *SignalHub) GetSubscriptionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscriptions)
}
