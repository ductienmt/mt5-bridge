package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"mt5-bridge/internal/models"
)

// FollowerRepository handles database operations for followers
type FollowerRepository struct {
	db *sql.DB
}

// NewFollowerRepository creates a new FollowerRepository
func NewFollowerRepository(db *sql.DB) *FollowerRepository {
	return &FollowerRepository{db: db}
}

// Create inserts a new follower into the database
func (r *FollowerRepository) Create(ctx context.Context, follower *models.Follower) error {
	query := `
		INSERT INTO followers (id, master_id, account_id, password_hash, server, status, lot_multiplier, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	now := time.Now()
	follower.CreatedAt = now
	follower.UpdatedAt = now
	if follower.Status == "" {
		follower.Status = string(models.FollowerStatusInactive)
	}
	if follower.LotMultiplier == 0 {
		follower.LotMultiplier = 1.0
	}

	_, err := r.db.ExecContext(ctx, query,
		follower.ID,
		follower.MasterID,
		follower.AccountID,
		follower.PasswordHash,
		follower.Server,
		follower.Status,
		follower.LotMultiplier,
		follower.CreatedAt,
		follower.UpdatedAt,
	)

	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			return ErrDuplicateFollower
		}
		return fmt.Errorf("failed to create follower: %w", err)
	}

	return nil
}

// GetByID retrieves a follower by ID
func (r *FollowerRepository) GetByID(ctx context.Context, id string) (*models.Follower, error) {
	query := `
		SELECT id, master_id, account_id, password_hash, server, status, lot_multiplier, deleted_at, created_at, updated_at
		FROM followers
		WHERE id = $1 AND deleted_at IS NULL
	`

	var follower models.Follower
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&follower.ID,
		&follower.MasterID,
		&follower.AccountID,
		&follower.PasswordHash,
		&follower.Server,
		&follower.Status,
		&follower.LotMultiplier,
		&follower.DeletedAt,
		&follower.CreatedAt,
		&follower.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get follower: %w", err)
	}

	return &follower, nil
}

// GetByMasterAndAccount retrieves a follower by master ID and account ID
func (r *FollowerRepository) GetByMasterAndAccount(ctx context.Context, masterID, accountID string) (*models.Follower, error) {
	query := `
		SELECT id, master_id, account_id, password_hash, server, status, lot_multiplier, deleted_at, created_at, updated_at
		FROM followers
		WHERE master_id = $1 AND account_id = $2 AND deleted_at IS NULL
	`

	var follower models.Follower
	err := r.db.QueryRowContext(ctx, query, masterID, accountID).Scan(
		&follower.ID,
		&follower.MasterID,
		&follower.AccountID,
		&follower.PasswordHash,
		&follower.Server,
		&follower.Status,
		&follower.LotMultiplier,
		&follower.DeletedAt,
		&follower.CreatedAt,
		&follower.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get follower: %w", err)
	}

	return &follower, nil
}

// GetByAccountID retrieves a follower by account ID
func (r *FollowerRepository) GetByAccountID(ctx context.Context, accountID string) (*models.Follower, error) {
	query := `
		SELECT id, master_id, account_id, password_hash, server, status, lot_multiplier, deleted_at, created_at, updated_at
		FROM followers
		WHERE account_id = $1 AND deleted_at IS NULL
	`

	var follower models.Follower
	err := r.db.QueryRowContext(ctx, query, accountID).Scan(
		&follower.ID,
		&follower.MasterID,
		&follower.AccountID,
		&follower.PasswordHash,
		&follower.Server,
		&follower.Status,
		&follower.LotMultiplier,
		&follower.DeletedAt,
		&follower.CreatedAt,
		&follower.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get follower by account ID: %w", err)
	}

	return &follower, nil
}

// UpdateStatus updates the status of a follower
func (r *FollowerRepository) UpdateStatus(ctx context.Context, id, status string) error {
	query := `
		UPDATE followers
		SET status = $1, updated_at = $2
		WHERE id = $3 AND deleted_at IS NULL
	`

	result, err := r.db.ExecContext(ctx, query, status, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update follower status: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}

	if rows == 0 {
		return ErrNotFound
	}

	return nil
}

// SoftDelete marks a follower as deleted
func (r *FollowerRepository) SoftDelete(ctx context.Context, id string) error {
	query := `
		UPDATE followers
		SET deleted_at = $1, updated_at = $2, status = 'inactive'
		WHERE id = $3 AND deleted_at IS NULL
	`

	result, err := r.db.ExecContext(ctx, query, time.Now(), time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to delete follower: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get affected rows: %w", err)
	}

	if rows == 0 {
		return ErrNotFound
	}

	return nil
}

// GetActiveFollowersByMaster returns all active followers for a master
func (r *FollowerRepository) GetActiveFollowersByMaster(ctx context.Context, masterID string) ([]*models.Follower, error) {
	query := `
		SELECT id, master_id, account_id, password_hash, server, status, lot_multiplier, deleted_at, created_at, updated_at
		FROM followers
		WHERE master_id = $1 AND status = 'active' AND deleted_at IS NULL
	`

	rows, err := r.db.QueryContext(ctx, query, masterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get active followers: %w", err)
	}
	defer rows.Close()

	var followers []*models.Follower
	for rows.Next() {
		var follower models.Follower
		if err := rows.Scan(
			&follower.ID,
			&follower.MasterID,
			&follower.AccountID,
			&follower.PasswordHash,
			&follower.Server,
			&follower.Status,
			&follower.LotMultiplier,
			&follower.DeletedAt,
			&follower.CreatedAt,
			&follower.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan follower: %w", err)
		}
		followers = append(followers, &follower)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating followers: %w", err)
	}

	return followers, nil
}

// GetAllFollowersByMaster returns all followers (active and inactive) for a master
func (r *FollowerRepository) GetAllFollowersByMaster(ctx context.Context, masterID string) ([]*models.Follower, error) {
	query := `
		SELECT id, master_id, account_id, password_hash, server, status, lot_multiplier, deleted_at, created_at, updated_at
		FROM followers
		WHERE master_id = $1 AND deleted_at IS NULL
	`

	rows, err := r.db.QueryContext(ctx, query, masterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get all followers: %w", err)
	}
	defer rows.Close()

	var followers []*models.Follower
	for rows.Next() {
		var follower models.Follower
		if err := rows.Scan(
			&follower.ID,
			&follower.MasterID,
			&follower.AccountID,
			&follower.PasswordHash,
			&follower.Server,
			&follower.Status,
			&follower.LotMultiplier,
			&follower.DeletedAt,
			&follower.CreatedAt,
			&follower.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan follower: %w", err)
		}
		followers = append(followers, &follower)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating followers: %w", err)
	}

	return followers, nil
}

// GetByAccountIDs returns followers by a list of account IDs
func (r *FollowerRepository) GetByAccountIDs(ctx context.Context, accountIDs []string) ([]*models.Follower, error) {
	if len(accountIDs) == 0 {
		return nil, nil
	}

	// Build placeholders
	placeholders := make([]string, len(accountIDs))
	args := make([]interface{}, len(accountIDs))
	for i, id := range accountIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, master_id, account_id, password_hash, server, status, lot_multiplier, deleted_at, created_at, updated_at
		FROM followers
		WHERE account_id IN (%s) AND status = 'active' AND deleted_at IS NULL
	`, strings.Join(placeholders, ","))

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get followers by account IDs: %w", err)
	}
	defer rows.Close()

	var followers []*models.Follower
	for rows.Next() {
		var follower models.Follower
		if err := rows.Scan(
			&follower.ID,
			&follower.MasterID,
			&follower.AccountID,
			&follower.PasswordHash,
			&follower.Server,
			&follower.Status,
			&follower.LotMultiplier,
			&follower.DeletedAt,
			&follower.CreatedAt,
			&follower.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan follower: %w", err)
		}
		followers = append(followers, &follower)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating followers: %w", err)
	}

	return followers, nil
}

// CountActive returns the count of active followers
func (r *FollowerRepository) CountActive(ctx context.Context) (int, error) {
	query := `SELECT COUNT(*) FROM followers WHERE status = 'active' AND deleted_at IS NULL`
	var count int
	err := r.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count active followers: %w", err)
	}
	return count, nil
}

// CountActiveByMaster returns the count of active followers for a master
func (r *FollowerRepository) CountActiveByMaster(ctx context.Context, masterID string) (int, error) {
	query := `SELECT COUNT(*) FROM followers WHERE master_id = $1 AND status = 'active' AND deleted_at IS NULL`
	var count int
	err := r.db.QueryRowContext(ctx, query, masterID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count active followers by master: %w", err)
	}
	return count, nil
}

// CountTotalByMaster returns the total count of followers for a master
func (r *FollowerRepository) CountTotalByMaster(ctx context.Context, masterID string) (int, error) {
	query := `SELECT COUNT(*) FROM followers WHERE master_id = $1 AND deleted_at IS NULL`
	var count int
	err := r.db.QueryRowContext(ctx, query, masterID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count total followers by master: %w", err)
	}
	return count, nil
}

// GetAll returns all followers (for syncing to Redis)
func (r *FollowerRepository) GetAll(ctx context.Context) ([]*models.Follower, error) {
	query := `
		SELECT id, master_id, account_id, password_hash, server, status, lot_multiplier, deleted_at, created_at, updated_at
		FROM followers
		WHERE deleted_at IS NULL
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all followers: %w", err)
	}
	defer rows.Close()

	var followers []*models.Follower
	for rows.Next() {
		var follower models.Follower
		if err := rows.Scan(
			&follower.ID,
			&follower.MasterID,
			&follower.AccountID,
			&follower.PasswordHash,
			&follower.Server,
			&follower.Status,
			&follower.LotMultiplier,
			&follower.DeletedAt,
			&follower.CreatedAt,
			&follower.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan follower: %w", err)
		}
		followers = append(followers, &follower)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating followers: %w", err)
	}

	return followers, nil
}
