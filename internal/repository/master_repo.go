package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"mt5-bridge/internal/models"
)

// MasterRepository handles database operations for masters
type MasterRepository struct {
	db *sql.DB
}

// NewMasterRepository creates a new MasterRepository
func NewMasterRepository(db *sql.DB) *MasterRepository {
	return &MasterRepository{db: db}
}

// Create inserts a new master into the database
func (r *MasterRepository) Create(ctx context.Context, master *models.Master) error {
	query := `
		INSERT INTO masters (id, account_id, password_hash, server, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	now := time.Now()
	master.CreatedAt = now
	master.UpdatedAt = now

	_, err := r.db.ExecContext(ctx, query,
		master.ID,
		master.AccountID,
		master.PasswordHash,
		master.Server,
		master.CreatedAt,
		master.UpdatedAt,
	)

	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			return ErrDuplicateAccountID
		}
		return fmt.Errorf("failed to create master: %w", err)
	}

	return nil
}

// GetByID retrieves a master by ID
func (r *MasterRepository) GetByID(ctx context.Context, id string) (*models.Master, error) {
	query := `
		SELECT id, account_id, password_hash, server, deleted_at, created_at, updated_at
		FROM masters
		WHERE id = $1 AND deleted_at IS NULL
	`

	var master models.Master
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&master.ID,
		&master.AccountID,
		&master.PasswordHash,
		&master.Server,
		&master.DeletedAt,
		&master.CreatedAt,
		&master.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get master: %w", err)
	}

	return &master, nil
}

// GetByAccountID retrieves a master by account ID
func (r *MasterRepository) GetByAccountID(ctx context.Context, accountID string) (*models.Master, error) {
	query := `
		SELECT id, account_id, password_hash, server, deleted_at, created_at, updated_at
		FROM masters
		WHERE account_id = $1 AND deleted_at IS NULL
	`

	var master models.Master
	err := r.db.QueryRowContext(ctx, query, accountID).Scan(
		&master.ID,
		&master.AccountID,
		&master.PasswordHash,
		&master.Server,
		&master.DeletedAt,
		&master.CreatedAt,
		&master.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get master by account ID: %w", err)
	}

	return &master, nil
}

// SoftDelete marks a master as deleted and returns affected follower count
func (r *MasterRepository) SoftDelete(ctx context.Context, id string) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First, deactivate all followers
	updateFollowersQuery := `
		UPDATE followers
		SET status = 'inactive', updated_at = $1
		WHERE master_id = $2 AND deleted_at IS NULL
	`
	now := time.Now()
	result, err := tx.ExecContext(ctx, updateFollowersQuery, now, id)
	if err != nil {
		return 0, fmt.Errorf("failed to deactivate followers: %w", err)
	}

	affectedFollowers, _ := result.RowsAffected()

	// Then soft delete the master
	deleteQuery := `
		UPDATE masters
		SET deleted_at = $1, updated_at = $2
		WHERE id = $3 AND deleted_at IS NULL
	`
	_, err = tx.ExecContext(ctx, deleteQuery, now, now, id)
	if err != nil {
		return 0, fmt.Errorf("failed to delete master: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return affectedFollowers, nil
}

// Exists checks if a master exists by ID
func (r *MasterRepository) Exists(ctx context.Context, id string) (bool, error) {
	query := `SELECT EXISTS(SELECT 1 FROM masters WHERE id = $1 AND deleted_at IS NULL)`
	var exists bool
	err := r.db.QueryRowContext(ctx, query, id).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check master existence: %w", err)
	}
	return exists, nil
}

// CountActive returns the count of active (non-deleted) masters
func (r *MasterRepository) CountActive(ctx context.Context) (int, error) {
	query := `SELECT COUNT(*) FROM masters WHERE deleted_at IS NULL`
	var count int
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count masters: %w", err)
	}
	return count, nil
}

// ListAll returns all non-deleted masters
func (r *MasterRepository) ListAll(ctx context.Context) ([]*models.Master, error) {
	query := `
		SELECT id, account_id, password_hash, server, deleted_at, created_at, updated_at
		FROM masters
		WHERE deleted_at IS NULL
		ORDER BY created_at DESC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list masters: %w", err)
	}
	defer rows.Close()

	var masters []*models.Master
	for rows.Next() {
		var master models.Master
		if err := rows.Scan(
			&master.ID,
			&master.AccountID,
			&master.PasswordHash,
			&master.Server,
			&master.DeletedAt,
			&master.CreatedAt,
			&master.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan master: %w", err)
		}
		masters = append(masters, &master)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating masters: %w", err)
	}

	return masters, nil
}
