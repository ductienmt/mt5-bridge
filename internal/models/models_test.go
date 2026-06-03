package models

import (
	"database/sql"
	"testing"
	"time"
)

func TestMaster_IsDeleted(t *testing.T) {
	tests := []struct {
		name     string
		deletedAt sql.NullTime
		expected bool
	}{
		{
			name:     "Not deleted",
			deletedAt: sql.NullTime{Valid: false},
			expected: false,
		},
		{
			name:     "Deleted",
			deletedAt: sql.NullTime{Valid: true, Time: time.Now()},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			master := &Master{DeletedAt: tt.deletedAt}
			if master.IsDeleted() != tt.expected {
				t.Errorf("IsDeleted() = %v, expected %v", master.IsDeleted(), tt.expected)
			}
		})
	}
}

func TestFollower_IsActive(t *testing.T) {
	tests := []struct {
		status   string
		expected bool
	}{
		{"active", true},
		{"inactive", false},
		{"", false},
		{"ACTIVE", false}, // Case sensitive
	}

	for _, tt := range tests {
		follower := &Follower{Status: tt.status}
		if follower.IsActive() != tt.expected {
			t.Errorf("IsActive() with status '%s' = %v, expected %v",
				tt.status, follower.IsActive(), tt.expected)
		}
	}
}

func TestFollower_IsDeleted(t *testing.T) {
	tests := []struct {
		name     string
		deletedAt sql.NullTime
		expected bool
	}{
		{
			name:     "Not deleted",
			deletedAt: sql.NullTime{Valid: false},
			expected: false,
		},
		{
			name:     "Deleted",
			deletedAt: sql.NullTime{Valid: true, Time: time.Now()},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			follower := &Follower{DeletedAt: tt.deletedAt}
			if follower.IsDeleted() != tt.expected {
				t.Errorf("IsDeleted() = %v, expected %v", follower.IsDeleted(), tt.expected)
			}
		})
	}
}

func TestNewErrorResponse(t *testing.T) {
	err := "Test error"
	code := "TEST_ERROR"
	details := map[string]string{"field": "value"}

	resp := NewErrorResponse(err, code, details)

	if resp.Error != err {
		t.Errorf("Error mismatch: got '%s', expected '%s'", resp.Error, err)
	}
	if resp.Code != code {
		t.Errorf("Code mismatch: got '%s', expected '%s'", resp.Code, code)
	}
	if resp.Details["field"] != "value" {
		t.Errorf("Details mismatch")
	}
	if resp.Timestamp == "" {
		t.Error("Timestamp should not be empty")
	}
}

func TestFollowerStatus_Constants(t *testing.T) {
	if FollowerStatusActive != "active" {
		t.Errorf("FollowerStatusActive should be 'active'")
	}
	if FollowerStatusInactive != "inactive" {
		t.Errorf("FollowerStatusInactive should be 'inactive'")
	}
}
