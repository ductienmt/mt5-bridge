package signal

import (
	"fmt"
	"sync"
	"time"
)

type Signal struct {
	Action string
	Side   string
	Symbol string
	Lot    float64
	SL     float64
	TP     float64
	Price  float64
	Magic  int64
	Pnl    float64
	Comment string
	Time   time.Time
}

func (s Signal) String() string {
	return fmt.Sprintf("%s %s %s lot=%.2f price=%.5f sl=%.5f tp=%.5f magic=%d pnl=%+.2f comment=%s",
		s.Action, s.Side, s.Symbol, s.Lot, s.Price, s.SL, s.TP, s.Magic, s.Pnl, s.Comment)
}

type Queue struct {
	mu      sync.Mutex
	signals []Signal
}

var q *Queue
var once sync.Once

func Get() *Queue {
	once.Do(func() { q = &Queue{} })
	return q
}

func (q *Queue) Push(s Signal) {
	q.mu.Lock()
	q.signals = append(q.signals, s)
	q.mu.Unlock()
}

func (q *Queue) Pop() (Signal, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.signals) == 0 {
		return Signal{}, false
	}
	s := q.signals[0]
	q.signals = q.signals[1:]
	return s, true
}

func (q *Queue) Drain() []Signal {
	q.mu.Lock()
	out := make([]Signal, len(q.signals))
	copy(out, q.signals)
	q.signals = q.signals[:0]
	q.mu.Unlock()
	return out
}

func (q *Queue) Size() int {
	q.mu.Lock()
	n := len(q.signals)
	q.mu.Unlock()
	return n
}

func (q *Queue) Clear() {
	q.mu.Lock()
	q.signals = q.signals[:0]
	q.mu.Unlock()
}
