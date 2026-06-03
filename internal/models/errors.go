package models

import (
	"time"
)

// ErrorResponse represents a standardized error response
type ErrorResponse struct {
	Error     string            `json:"error"`
	Code      string            `json:"code"`
	Details   map[string]string `json:"details,omitempty"`
	Timestamp string            `json:"timestamp"`
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status     string            `json:"status"`
	Version    string            `json:"version"`
	Components map[string]string `json:"components"`
	Stats      HealthStats       `json:"stats"`
}

// HealthStats represents system health statistics
type HealthStats struct {
	ActiveMasters   int `json:"activeMasters"`
	ActiveFollowers int `json:"activeFollowers"`
	QueueSize      int `json:"queueSize"`
}

// NewErrorResponse creates a new error response
func NewErrorResponse(err string, code string, details map[string]string) ErrorResponse {
	return ErrorResponse{
		Error:     err,
		Code:      code,
		Details:   details,
		Timestamp: time.Now().Format(time.RFC3339),
	}
}

// LoginRequest represents the login request
type LoginRequest struct {
	Username string `json:"username" binding:"required,min=3,max=50"`
	Password string `json:"password" binding:"required,min=6"`
}

// LoginResponse represents the login response with JWT token
type LoginResponse struct {
	AccessToken string `json:"accessToken"`
	TokenType  string `json:"tokenType"`
	ExpiresIn  int64  `json:"expiresIn"`
	User       UserInfo `json:"user"`
}

// UserInfo represents basic user information
type UserInfo struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
	Role     string `json:"role"`
}
