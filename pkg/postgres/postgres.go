package postgres

import (
	"context"
	"fmt"
	"time"

	"thor/pkg/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type IDB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, dest []any, args ...any) error
	Tx(ctx context.Context, fn func(tx pgx.Tx) error) error
}

type DB struct {
	write    *pgxpool.Pool
	read     *pgxpool.Pool
	log      *zap.Logger
	writeCfg *config.PostgresConfig
	readCfg  *config.PostgresConfig
}

func (db *DB) HealthCheck(ctx context.Context) error {
	writeCtx, writeCancel := context.WithTimeout(ctx, db.writeCfg.Timeout)
	defer writeCancel()
	if err := db.write.Ping(writeCtx); err != nil {
		return fmt.Errorf("write pool ping: %w", err)
	}

	readCtx, readCancel := context.WithTimeout(ctx, db.readCfg.Timeout)
	defer readCancel()
	if err := db.read.Ping(readCtx); err != nil {
		return fmt.Errorf("read pool ping: %w", err)
	}
	return nil
}

func (db *DB) Close() {
	if db.read != nil {
		db.read.Close()
	}
	if db.write != nil {
		db.write.Close()
	}
}

func (db *DB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	ctx, cancel := context.WithTimeout(ctx, db.writeCfg.Timeout)
	defer cancel()
	tag, err := db.write.Exec(ctx, sql, args...)
	if err != nil {
		db.log.Error("Execute failed", zap.Error(err))
	}
	return tag, err
}

func (db *DB) QueryRow(ctx context.Context, sql string, dest []any, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, db.readCfg.Timeout)
	defer cancel()
	err := db.read.QueryRow(ctx, sql, args...).Scan(dest...)
	if err != nil {
		db.log.Error("Query failed", zap.Error(err))
	}
	return err
}

func (db *DB) Tx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, db.writeCfg.Timeout)
	defer cancel()

	tx, err := db.write.Begin(ctx)
	if err != nil {
		db.log.Error("Begin Transaction failed", zap.Error(err))
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		tx.Rollback(ctx)
	}()

	if err = fn(tx); err != nil {
		db.log.Error("Transaction failed", zap.Error(err))
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		db.log.Error("Transaction commit failed", zap.Error(err))
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func New(ctx context.Context, readCfg *config.PostgresConfig, writeCfg *config.PostgresConfig, log *zap.Logger) (*DB, error) {
	writePool, err := newPool(ctx, writeCfg, log, "write")
	if err != nil {
		return nil, err
	}

	readPool, err := newPool(ctx, readCfg, log, "read")
	if err != nil {
		writePool.Close()
		return nil, err
	}

	return &DB{write: writePool, read: readPool, readCfg: readCfg, writeCfg: writeCfg, log: log}, nil
}

func getDSN(cfg *config.PostgresConfig) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode)
}

func newPool(ctx context.Context, cfg *config.PostgresConfig, log *zap.Logger, role string) (*pgxpool.Pool, error) {
	dsn := getDSN(cfg)
	pgxConfig, err := pgxpool.ParseConfig(dsn)

	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	if cfg.MaxConns > 0 {
		pgxConfig.MaxConns = cfg.MaxConns
	}

	if cfg.MinConns > 0 {
		pgxConfig.MinConns = cfg.MinConns
	}

	if cfg.MaxConnIdleTime > 0 {
		pgxConfig.MaxConnIdleTime = cfg.MaxConnIdleTime
	}

	if cfg.MaxConnLifeTime > 0 {
		pgxConfig.MaxConnLifetime = cfg.MaxConnLifeTime
	}

	retries := cfg.Retries
	if retries <= 0 {
		retries = 1
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		log.Error("Postgres create pool failed", zap.Error(err))
		return nil, fmt.Errorf("Postgres create pool failed: %w", err)
	}

	for i := 0; i < retries; i++ {
		if err = pool.Ping(ctx); err == nil {
			log.Info("Postgres connected", zap.String("role", role))
			return pool, nil
		}

		log.Warn("Postgres connection failed, retrying", zap.String("role", role), zap.Int("attempt", i+1), zap.Error(err))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(cfg.RetryBackoffMs) * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("postgres %s: %w", role, err)
}
