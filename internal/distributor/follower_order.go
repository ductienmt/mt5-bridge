package distributor

import (
	"math"

	"mt5-bridge/internal/models"
	"mt5-bridge/signal"
)

const (
	// MinLot is the minimum lot size
	MinLot = 0.01
	// MaxLot is the maximum lot size
	MaxLot = 100.0
	// LotStep is the rounding step for lot sizes
	LotStep = 0.01
)

// CreateFollowerOrder creates a follower order from a master signal
func CreateFollowerOrder(follower *models.Follower, masterSignal signal.Signal) *models.FollowerOrder {
	// Calculate adjusted lot size
	adjustedLot := CalculateLot(masterSignal.Lot, follower.LotMultiplier)

	// Create the follower signal
	followerSignal := signal.Signal{
		Action:  masterSignal.Action,
		Side:    masterSignal.Side,
		Symbol:  masterSignal.Symbol,
		Lot:     adjustedLot,
		Price:   masterSignal.Price,
		SL:      masterSignal.SL,
		TP:      masterSignal.TP,
		Magic:   masterSignal.Magic,
		Pnl:     masterSignal.Pnl,
		Comment: masterSignal.Comment,
		Time:    masterSignal.Time,
	}

	return &models.FollowerOrder{
		FollowerAccountID: follower.AccountID,
		Password:         follower.PasswordHash, // Note: in real impl, use secure transfer
		Server:           follower.Server,
		LotMultiplier:    follower.LotMultiplier,
		Signal:           followerSignal,
	}
}

// CalculateLot calculates the follower lot size based on master lot and multiplier
// Formula: round(masterLot * multiplier, 0.01)
func CalculateLot(masterLot, multiplier float64) float64 {
	rawLot := masterLot * multiplier
	
	// Round to nearest 0.01
	roundedLot := math.Round(rawLot*100) / 100
	
	// Clamp to valid range
	if roundedLot < MinLot {
		roundedLot = MinLot
	}
	if roundedLot > MaxLot {
		roundedLot = MaxLot
	}
	
	return roundedLot
}

// ValidateLotMultiplier checks if a lot multiplier is within valid range
func ValidateLotMultiplier(multiplier float64) bool {
	return multiplier >= 0.01 && multiplier <= 100.0
}

// CalculateReverseLot calculates lot for reverse copy trading
// (e.g., when master BUY, follower SELL)
func CalculateReverseLot(masterLot, multiplier float64) float64 {
	return CalculateLot(masterLot, multiplier)
}

// CopySignalFields copies all fields from master signal to follower signal except lot
func CopySignalFields(masterSignal, followerSignal signal.Signal) signal.Signal {
	followerSignal.Action = masterSignal.Action
	followerSignal.Side = masterSignal.Side
	followerSignal.Symbol = masterSignal.Symbol
	followerSignal.Price = masterSignal.Price
	followerSignal.SL = masterSignal.SL
	followerSignal.TP = masterSignal.TP
	followerSignal.Magic = masterSignal.Magic
	followerSignal.Pnl = masterSignal.Pnl
	followerSignal.Comment = masterSignal.Comment
	followerSignal.Time = masterSignal.Time
	return followerSignal
}

// ReverseSignalSide reverses the side of a signal
func ReverseSignalSide(side string) string {
	switch side {
	case "BUY":
		return "SELL"
	case "SELL":
		return "BUY"
	case "BUY_STOP":
		return "SELL_STOP"
	case "SELL_STOP":
		return "BUY_STOP"
	case "BUY_LIMIT":
		return "SELL_LIMIT"
	case "SELL_LIMIT":
		return "BUY_LIMIT"
	default:
		return side
	}
}
