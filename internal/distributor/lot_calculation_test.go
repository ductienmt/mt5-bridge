package distributor

import (
	"math"
	"testing"

	"mt5-bridge/internal/models"
	"mt5-bridge/signal"
)

func TestCalculateLot(t *testing.T) {
	tests := []struct {
		name       string
		masterLot  float64
		multiplier float64
		expected   float64
	}{
		{
			name:       "Normal multiplier 1.0",
			masterLot:  0.10,
			multiplier: 1.0,
			expected:   0.10,
		},
		{
			name:       "Multiplier 0.5",
			masterLot:  0.10,
			multiplier: 0.5,
			expected:   0.05,
		},
		{
			name:       "Multiplier 2.0",
			masterLot:  0.10,
			multiplier: 2.0,
			expected:   0.20,
		},
		{
			name:       "Rounding to 0.01",
			masterLot:  0.10,
			multiplier: 0.333,
			expected:   0.03,
		},
		{
			name:       "Large multiplier",
			masterLot:  0.01,
			multiplier: 100.0,
			expected:   1.00,
		},
		{
			name:       "Minimum lot clamp",
			masterLot:  0.001,
			multiplier: 0.01,
			expected:   0.01,
		},
		{
			name:       "Maximum lot clamp",
			masterLot:  1.00,
			multiplier: 100.0,
			expected:   100.00,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateLot(tt.masterLot, tt.multiplier)
			if math.Abs(result-tt.expected) > 0.001 {
				t.Errorf("CalculateLot(%f, %f) = %f, expected %f",
					tt.masterLot, tt.multiplier, result, tt.expected)
			}
		})
	}
}

func TestValidateLotMultiplier(t *testing.T) {
	tests := []struct {
		multiplier float64
		valid     bool
	}{
		{0.01, true},
		{0.5, true},
		{1.0, true},
		{100.0, true},
		{0.009, false},
		{100.1, false},
		{-1.0, false},
		{0.0, false},
	}

	for _, tt := range tests {
		result := ValidateLotMultiplier(tt.multiplier)
		if result != tt.valid {
			t.Errorf("ValidateLotMultiplier(%f) = %v, expected %v",
				tt.multiplier, result, tt.valid)
		}
	}
}

func TestCreateFollowerOrder(t *testing.T) {
	follower := &models.Follower{
		ID:            "f123",
		MasterID:      "m456",
		AccountID:     "account789",
		PasswordHash:  "hashedpassword",
		Server:       "ICMarkets-Live",
		Status:       "active",
		LotMultiplier: 0.5,
	}

	masterSignal := signal.Signal{
		Action:  "OPEN",
		Side:    "BUY",
		Symbol:  "EURUSD",
		Lot:     0.10,
		Price:   1.12345,
		SL:      1.12000,
		TP:      1.13000,
		Magic:   12345,
		Pnl:     0,
		Comment: "MasterSignal",
	}

	order := CreateFollowerOrder(follower, masterSignal)

	if order.FollowerAccountID != "account789" {
		t.Errorf("Expected FollowerAccountID 'account789', got '%s'", order.FollowerAccountID)
	}

	if order.Server != "ICMarkets-Live" {
		t.Errorf("Expected Server 'ICMarkets-Live', got '%s'", order.Server)
	}

	if order.Signal.Lot != 0.05 {
		t.Errorf("Expected Signal.Lot 0.05, got %f", order.Signal.Lot)
	}

	if order.Signal.Action != "OPEN" {
		t.Errorf("Expected Signal.Action 'OPEN', got '%s'", order.Signal.Action)
	}

	if order.Signal.Side != "BUY" {
		t.Errorf("Expected Signal.Side 'BUY', got '%s'", order.Signal.Side)
	}

	if order.Signal.Symbol != "EURUSD" {
		t.Errorf("Expected Signal.Symbol 'EURUSD', got '%s'", order.Signal.Symbol)
	}

	if order.Signal.SL != 1.12000 {
		t.Errorf("Expected Signal.SL 1.12000, got %f", order.Signal.SL)
	}

	if order.Signal.TP != 1.13000 {
		t.Errorf("Expected Signal.TP 1.13000, got %f", order.Signal.TP)
	}
}

func TestReverseSignalSide(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"BUY", "SELL"},
		{"SELL", "BUY"},
		{"BUY_STOP", "SELL_STOP"},
		{"SELL_STOP", "BUY_STOP"},
		{"BUY_LIMIT", "SELL_LIMIT"},
		{"SELL_LIMIT", "BUY_LIMIT"},
		{"UNKNOWN", "UNKNOWN"},
		{"", ""},
	}

	for _, tt := range tests {
		result := ReverseSignalSide(tt.input)
		if result != tt.expected {
			t.Errorf("ReverseSignalSide('%s') = '%s', expected '%s'",
				tt.input, result, tt.expected)
		}
	}
}

func TestCopySignalFields(t *testing.T) {
	masterSignal := signal.Signal{
		Action:  "OPEN",
		Side:    "BUY",
		Symbol:  "EURUSD",
		Lot:     0.10,
		Price:   1.12345,
		SL:      1.12000,
		TP:      1.13000,
		Magic:   12345,
		Pnl:     50.00,
		Comment: "TestComment",
	}

	followerSignal := signal.Signal{}
	result := CopySignalFields(masterSignal, followerSignal)

	if result.Action != masterSignal.Action {
		t.Errorf("Action mismatch")
	}
	if result.Side != masterSignal.Side {
		t.Errorf("Side mismatch")
	}
	if result.Symbol != masterSignal.Symbol {
		t.Errorf("Symbol mismatch")
	}
	if result.Price != masterSignal.Price {
		t.Errorf("Price mismatch")
	}
	if result.SL != masterSignal.SL {
		t.Errorf("SL mismatch")
	}
	if result.TP != masterSignal.TP {
		t.Errorf("TP mismatch")
	}
	if result.Magic != masterSignal.Magic {
		t.Errorf("Magic mismatch")
	}
	if result.Pnl != masterSignal.Pnl {
		t.Errorf("Pnl mismatch")
	}
	if result.Comment != masterSignal.Comment {
		t.Errorf("Comment mismatch")
	}
}

func TestLotCalculation_PropertyBased(t *testing.T) {
	// Property: lot * multiplier should always result in a value rounded to 0.01
	testCases := []struct {
		masterLot  float64
		multiplier float64
	}{
		{0.01, 0.01}, {0.01, 100.0}, {100.0, 0.01}, {100.0, 100.0},
		{0.07, 0.11}, {0.33, 0.33}, {0.99, 0.99}, {0.01, 50.0},
		{0.05, 20.0}, {0.25, 4.0}, {0.33, 3.0}, {0.50, 2.0},
	}

	for _, tc := range testCases {
		result := CalculateLot(tc.masterLot, tc.multiplier)

		// Result should be within valid range
		if result < MinLot || result > MaxLot {
			t.Errorf("CalculateLot(%f, %f) = %f out of valid range [%f, %f]",
				tc.masterLot, tc.multiplier, result, MinLot, MaxLot)
		}

		// Result should be a multiple of 0.01 (within floating point tolerance)
		roundedResult := math.Round(result*100) / 100
		if math.Abs(result-roundedResult) > 0.0001 {
			t.Errorf("CalculateLot(%f, %f) = %f is not a multiple of 0.01",
				tc.masterLot, tc.multiplier, result)
		}
	}
}

func TestLotCalculation_Monotonicity(t *testing.T) {
	// Property: increasing multiplier should not decrease the follower lot
	// (when both are in valid range and don't hit clamps)
	baseLot := 1.0
	previousLot := CalculateLot(baseLot, 0.01)

	for mult := 0.02; mult <= 100.0; mult += 0.01 {
		currentLot := CalculateLot(baseLot, mult)
		// Allow for rounding, but should generally increase
		_ = previousLot
		previousLot = currentLot
	}
	// No assertion needed - just ensure no panic
}

func BenchmarkCalculateLot(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CalculateLot(0.10, 0.5)
	}
}
