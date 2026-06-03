package models

import (
	"time"
)

// SignalAction represents the type of trading action
type SignalAction string

const (
	SignalActionOpen  SignalAction = "OPEN"
	SignalActionClose SignalAction = "CLOSE"
	SignalActionEdit  SignalAction = "EDIT"
)

// SignalSide represents the trading direction
type SignalSide string

const (
	SignalSideBuy       SignalSide = "BUY"
	SignalSideSell      SignalSide = "SELL"
	SignalSideBuyStop   SignalSide = "BUY_STOP"
	SignalSideSellStop  SignalSide = "SELL_STOP"
	SignalSideBuyLimit  SignalSide = "BUY_LIMIT"
	SignalSideSellLimit SignalSide = "SELL_LIMIT"
)

// MasterSignal represents a trading signal from a master account
type MasterSignal struct {
	AccountID string       `json:"accountId"`
	Action    SignalAction `json:"action"`
	Side      SignalSide   `json:"side"`
	Symbol    string       `json:"symbol"`
	Lot       float64      `json:"lot"`
	Price     float64      `json:"price"`
	SL        float64      `json:"sl"`
	TP        float64      `json:"tp"`
	Magic     int64        `json:"magic"`
	Pnl       float64      `json:"pnl"`
	Comment   string       `json:"comment"`
	Time      time.Time    `json:"time"`
}

// DistributedSignal represents a signal that has been distributed to followers
type DistributedSignal struct {
	Signal        MasterSignal `json:"signal"`
	MasterID      string       `json:"masterId"`
	FollowerCount int          `json:"followerCount"`
	LatencyMs     int64        `json:"latencyMs"`
	DistributedAt time.Time    `json:"distributedAt"`
}

// ToSignal converts a DistributedSignal to MasterSignal
func (ds *DistributedSignal) ToSignal() MasterSignal {
	return ds.Signal
}
