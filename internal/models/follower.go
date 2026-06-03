package models

import (
	"database/sql"
	"time"

	sigpkg "mt5-bridge/signal"
)

// FollowerStatus represents the status of a follower
type FollowerStatus string

const (
	FollowerStatusActive   FollowerStatus = "active"
	FollowerStatusInactive FollowerStatus = "inactive"
)

// Follower represents a follower trading account that receives signals from a master
type Follower struct {
	ID            string       `json:"id" db:"id"`
	MasterID      string       `json:"masterId" db:"master_id"`
	AccountID    string       `json:"accountId" db:"account_id"`
	PasswordHash string      `json:"-" db:"password_hash"`
	Server       string       `json:"server" db:"server"`
	Status       string       `json:"status" db:"status"`
	LotMultiplier float64     `json:"lotMultiplier" db:"lot_multiplier"`
	DeletedAt    sql.NullTime `json:"deletedAt,omitempty" db:"deleted_at"`
	CreatedAt    time.Time    `json:"createdAt" db:"created_at"`
	UpdatedAt    time.Time    `json:"updatedAt" db:"updated_at"`
}

// IsActive returns true if the follower is actively copying
func (f *Follower) IsActive() bool {
	return f.Status == string(FollowerStatusActive)
}

// IsDeleted returns true if the follower has been soft deleted
func (f *Follower) IsDeleted() bool {
	return f.DeletedAt.Valid
}

// CreateFollowerRequest represents the request to register a new follower
type CreateFollowerRequest struct {
	AccountID     string  `json:"accountId" binding:"required,min=4,max=50"`
	Password      string  `json:"password" binding:"required,min=8,max=72"`
	Server        string  `json:"server" binding:"required,min=1,max=100"`
	LotMultiplier float64 `json:"lotMultiplier" binding:"required,min=0.01,max=100.0"`
}

// CreateFollowerResponse represents the response after creating a follower
type CreateFollowerResponse struct {
	FollowerID   string    `json:"followerId"`
	MasterID    string    `json:"masterId"`
	AccountID   string    `json:"accountId"`
	Server      string    `json:"server"`
	Status      string    `json:"status"`
	LotMultiplier float64 `json:"lotMultiplier"`
	CreatedAt   time.Time `json:"createdAt"`
}

// StartStopResponse represents the response for start/stop operations
type StartStopResponse struct {
	FollowerID string `json:"followerId"`
	MasterID   string `json:"masterId"`
	Status     string `json:"status"`
	Message    string `json:"message"`
}

// DeleteFollowerResponse represents the response after deleting a follower
type DeleteFollowerResponse struct {
	FollowerID string `json:"followerId"`
	Message    string `json:"message"`
}

// FollowerOrder represents an order to be sent to a follower account
type FollowerOrder struct {
	FollowerAccountID string          `json:"followerAccountId"`
	Password        string          `json:"password"`
	Server          string          `json:"server"`
	LotMultiplier   float64         `json:"lotMultiplier"`
	Signal         sigpkg.Signal   `json:"signal"`
}
