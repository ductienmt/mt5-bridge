package signal

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type RawSignal struct {
	Action  string `json:"action"`
	Side   string `json:"side"`
	Symbol string `json:"symbol"`
	Lot    any    `json:"lot"`
	SL     any    `json:"sl"`
	TP     any    `json:"tp"`
	Price  any    `json:"price"`
	Magic  any    `json:"magic"`
	Pnl    any    `json:"pnl"`
	Comment string `json:"comment"`
}

func ParseSignal(data []byte) (Signal, error) {
	var raw RawSignal
	if err := json.Unmarshal(data, &raw); err != nil {
		return Signal{}, fmt.Errorf("JSON parse error: %w", err)
	}

	s := Signal{
		Action:  strings.ToUpper(strings.TrimSpace(raw.Action)),
		Side:   strings.ToUpper(strings.TrimSpace(raw.Side)),
		Symbol: strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		Comment: strings.TrimSpace(raw.Comment),
		Time:    time.Now(),
	}

	s.Lot   = parseFloat(raw.Lot)
	s.SL    = parseFloat(raw.SL)
	s.TP    = parseFloat(raw.TP)
	s.Price = parseFloat(raw.Price)
	s.Magic = parseInt(raw.Magic)
	s.Pnl   = parseFloat(raw.Pnl)

	return s, nil
}

func parseFloat(v any) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(val), 64)
		return f
	}
	return 0
}

func parseInt(v any) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	case string:
		i, _ := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
		return i
	}
	return 0
}
