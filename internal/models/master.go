package models

import (
	"database/sql"
	"time"
)

// Master represents a master trading account that broadcasts signals
type Master struct {
	ID           string       `json:"id" db:"id"`
	AccountID   string       `json:"accountId" db:"account_id"`
	PasswordHash string      `json:"-" db:"password_hash"`
	Server      string       `json:"server" db:"server"`
	DeletedAt   sql.NullTime `json:"deletedAt,omitempty" db:"deleted_at"`
	CreatedAt   time.Time    `json:"createdAt" db:"created_at"`
	UpdatedAt   time.Time    `json:"updatedAt" db:"updated_at"`
}

// IsDeleted returns true if the master has been soft deleted
func (m *Master) IsDeleted() bool {
	return m.DeletedAt.Valid
}

// CreateMasterRequest represents the request to create a new master
type CreateMasterRequest struct {
	AccountID string `json:"accountId" binding:"required,min=4,max=50"`
	Password  string `json:"password" binding:"required,min=8,max=72"`
	Server    string `json:"server" binding:"required,min=1,max=100"`
}

// CreateMasterResponse represents the response after creating a master
type CreateMasterResponse struct {
	MasterID  string    `json:"masterId"`
	AccountID string    `json:"accountId"`
	Server    string    `json:"server"`
	CreatedAt time.Time `json:"createdAt"`
}

// DeleteMasterResponse represents the response after deleting a master
type DeleteMasterResponse struct {
	MasterID string `json:"masterId"`
	Message  string `json:"message"`
}

// MasterStats represents statistics for a master account
type MasterStats struct {
	MasterID        string `json:"masterId"`
	ActiveFollowers int    `json:"activeFollowers"`
	TotalFollowers  int    `json:"totalFollowers"`
}
