package postgres

import (
	"context"
	"fmt"
	"loki/pkg/config"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Write        *pgxpool.Pool
	Read         *pgxpool.Pool
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (db *DB) HealthCheck(ctx context.Context) error {
	writeCtx, writeCancel := context.WithTimeout(ctx, db.WriteTimeout)
	defer writeCancel()
	if err := db.Write.Ping(writeCtx); err != nil {
		return fmt.Errorf("write pool ping %w", err)
	}

	readCtx, readCancel := context.WithTimeout(ctx, db.ReadTimeout)
	defer readCancel()
	if err := db.Read.Ping(readCtx); err != nil {
		return fmt.Errorf("read pool ping %w", err)
	}
	return nil
}

func (db *DB) Close() {
	if db.Read != nil {
		db.Read.Close()
	}
	if db.Write != nil {
		db.Write.Close()
	}
}

func (db *DB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	ctx, cancel := context.WithTimeout(ctx, db.WriteTimeout)
	defer cancel()
	return db.Write.Exec(ctx, sql, args...)
}

func (db *DB) QueryRow(ctx context.Context, dest []any, sql string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, db.ReadTimeout)
	defer cancel()
	return db.Read.QueryRow(ctx, sql, args...).Scan(dest...)
}

func (db *DB) Tx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, db.WriteTimeout)
	defer cancel()

	tx, err := db.Write.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func NewDB(ctx context.Context, readCfg *config.PostgresConfig, writeCfg *config.PostgresConfig) (*DB, error) {
	writePool, err := newPool(ctx, writeCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres %s: %w", "write", err)
	}
	readPool, err := newPool(ctx, readCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres %s: %w", "read", err)
	}

	return &DB{Write: writePool, Read: readPool, ReadTimeout: readCfg.Timeout, WriteTimeout: writeCfg.Timeout}, nil
}

func getDSN(cfg *config.PostgresConfig) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode)
}

func newPool(ctx context.Context, cfg *config.PostgresConfig) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

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

	return pgxpool.NewWithConfig(ctx, pgxConfig)
}
