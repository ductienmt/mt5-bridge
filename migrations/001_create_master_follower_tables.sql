-- Migration: 001_create_master_follower_tables
-- Description: Create masters and followers tables for copy trading system
-- Created: 2024-01-15

-- Enable UUID extension if not exists
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Masters table: stores master trading account information
CREATE TABLE IF NOT EXISTS masters (
    id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    server VARCHAR(100) NOT NULL,
    deleted_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_account_id_length CHECK (LENGTH(account_id) >= 4 AND LENGTH(account_id) <= 50)
);

-- Index for fast master signal identification lookup
CREATE INDEX IF NOT EXISTS idx_masters_account_id ON masters(account_id);
CREATE INDEX IF NOT EXISTS idx_masters_deleted_at ON masters(deleted_at);

-- Followers table: stores follower trading account information
CREATE TABLE IF NOT EXISTS followers (
    id VARCHAR(255) PRIMARY KEY,
    master_id VARCHAR(255) NOT NULL,
    account_id VARCHAR(50) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    server VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'inactive' CHECK (status IN ('active', 'inactive')),
    lot_multiplier DECIMAL(6,2) NOT NULL DEFAULT 1.0,
    deleted_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (master_id) REFERENCES masters(id) ON DELETE CASCADE,
    CONSTRAINT unique_follower_per_master UNIQUE (master_id, account_id),
    CONSTRAINT chk_lot_multiplier_range CHECK (lot_multiplier >= 0.01 AND lot_multiplier <= 100.0),
    CONSTRAINT chk_follower_account_id_length CHECK (LENGTH(account_id) >= 4 AND LENGTH(account_id) <= 50)
);

-- Index for fast retrieval of active followers per master
CREATE INDEX IF NOT EXISTS idx_followers_master_id ON followers(master_id);
CREATE INDEX IF NOT EXISTS idx_followers_master_status ON followers(master_id, status);
CREATE INDEX IF NOT EXISTS idx_followers_deleted_at ON followers(deleted_at);
CREATE INDEX IF NOT EXISTS idx_followers_account_id ON followers(account_id);

-- Signal history table: stores master signal history for analysis
CREATE TABLE IF NOT EXISTS signal_history (
    id BIGSERIAL PRIMARY KEY,
    master_id VARCHAR(255) NOT NULL,
    account_id VARCHAR(50) NOT NULL,
    action VARCHAR(20) NOT NULL,
    side VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    lot DECIMAL(10,4) NOT NULL,
    price DECIMAL(15,5) NOT NULL,
    sl DECIMAL(15,5) DEFAULT 0,
    tp DECIMAL(15,5) DEFAULT 0,
    magic BIGINT DEFAULT 0,
    pnl DECIMAL(15,5) DEFAULT 0,
    comment TEXT DEFAULT '',
    signal_time TIMESTAMP NOT NULL,
    distributed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    follower_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for signal history queries
CREATE INDEX IF NOT EXISTS idx_signal_history_master_id ON signal_history(master_id);
CREATE INDEX IF NOT EXISTS idx_signal_history_signal_time ON signal_history(signal_time DESC);
CREATE INDEX IF NOT EXISTS idx_signal_history_symbol ON signal_history(symbol);
CREATE INDEX IF NOT EXISTS idx_signal_history_account_id ON signal_history(account_id);

-- Comments for documentation
COMMENT ON TABLE masters IS 'Master trading accounts that broadcast signals';
COMMENT ON TABLE followers IS 'Follower accounts that receive and execute signals from masters';
COMMENT ON TABLE signal_history IS 'Historical record of signals distributed from masters';
COMMENT ON COLUMN masters.account_id IS 'MT5 account ID of the master trader';
COMMENT ON COLUMN followers.master_id IS 'Reference to the master account this follower subscribes to';
COMMENT ON COLUMN followers.lot_multiplier IS 'Multiplier applied to master lot size (0.01 - 100.0)';
COMMENT ON COLUMN followers.status IS 'Whether the follower is actively copying (active/inactive)';

-- Trigger function to auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to masters table
DROP TRIGGER IF EXISTS update_masters_updated_at ON masters;
CREATE TRIGGER update_masters_updated_at
    BEFORE UPDATE ON masters
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to followers table
DROP TRIGGER IF EXISTS update_followers_updated_at ON followers;
CREATE TRIGGER update_followers_updated_at
    BEFORE UPDATE ON followers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
