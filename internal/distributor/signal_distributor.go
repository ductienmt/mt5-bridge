package distributor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"mt5-bridge/internal/models"
	"mt5-bridge/signal"
)

// SignalDistributor handles distributing signals from masters to followers
type SignalDistributor struct {
	followerOrders chan *models.FollowerOrder
	workerCount    int
}

// NewSignalDistributor creates a new SignalDistributor
func NewSignalDistributor(workerCount int) *SignalDistributor {
	if workerCount <= 0 {
		workerCount = 10
	}
	return &SignalDistributor{
		followerOrders: make(chan *models.FollowerOrder, 1000),
		workerCount:    workerCount,
	}
}

// DistributeResult represents the result of a distribution operation
type DistributeResult struct {
	TotalFollowers int
	SuccessCount  int
	ErrorCount    int
	LatencyMs     int64
}

// Distribute distributes a master signal to all active followers
func (sd *SignalDistributor) Distribute(
	masterID string,
	masterSignal signal.Signal,
	followers []*models.Follower,
) DistributeResult {
	startTime := time.Now()
	
	result := DistributeResult{
		TotalFollowers: len(followers),
	}

	if len(followers) == 0 {
		return result
	}

	// Create follower orders with lot multipliers
	var wg sync.WaitGroup
	successCount := atomic.Int32{}
	errorCount := atomic.Int32{}

	for _, follower := range followers {
		wg.Add(1)
		go func(f *models.Follower) {
			defer wg.Done()

			order := CreateFollowerOrder(f, masterSignal)
			if err := sd.enqueueOrder(order); err != nil {
				// Log error (in real implementation, use proper logging)
				errorCount.Add(1)
				return
			}
			successCount.Add(1)
		}(follower)
	}

	wg.Wait()

	result.SuccessCount = int(successCount.Load())
	result.ErrorCount = int(errorCount.Load())
	result.LatencyMs = time.Since(startTime).Milliseconds()

	return result
}

// enqueueOrder sends the follower order to the queue
func (sd *SignalDistributor) enqueueOrder(order *models.FollowerOrder) error {
	select {
	case sd.followerOrders <- order:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout enqueueing order for follower %s", order.FollowerAccountID)
	}
}

// GetQueueChannel returns the channel for follower orders
func (sd *SignalDistributor) GetQueueChannel() <-chan *models.FollowerOrder {
	return sd.followerOrders
}

// Close closes the distributor
func (sd *SignalDistributor) Close() {
	close(sd.followerOrders)
}
