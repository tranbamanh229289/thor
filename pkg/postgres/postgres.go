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

type DB struct {
	write        *pgxpool.Pool
	read         *pgxpool.Pool
	log          *zap.Logger
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (db *DB) HealthCheck(ctx context.Context) error {
	writeCtx, writeCancel := context.WithTimeout(ctx, db.writeTimeout)
	defer writeCancel()
	if err := db.write.Ping(writeCtx); err != nil {
		return fmt.Errorf("write pool ping: %w", err)
	}

	readCtx, readCancel := context.WithTimeout(ctx, db.readTimeout)
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
	ctx, cancel := context.WithTimeout(ctx, db.writeTimeout)
	defer cancel()
	tag, err := db.write.Exec(ctx, sql, args...)
	if err != nil {
		db.log.Error("Execute failed", zap.Error(err))
	}
	return tag, err
}

func (db *DB) QueryRow(ctx context.Context, dest []any, sql string, args ...any) error {
	ctx, cancel := context.WithTimeout(ctx, db.readTimeout)
	defer cancel()
	err := db.read.QueryRow(ctx, sql, args...).Scan(dest...)
	if err != nil {
		db.log.Error("Query failed", zap.Error(err))
	}
	return err
}

func (db *DB) Tx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, db.writeTimeout)
	defer cancel()

	tx, err := db.write.Begin(ctx)
	if err != nil {

		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		tx.Rollback(ctx)
	}()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

func New(ctx context.Context, readCfg *config.PostgresConfig, writeCfg *config.PostgresConfig, log *zap.Logger) (*DB, error) {
	writePool, err := newPool(ctx, writeCfg)
	if err != nil {
		log.Error("Postgres connection failed", zap.String("role", "write"), zap.Error(err))
		return nil, fmt.Errorf("postgres %s: %w", "write", err)
	} else {
		log.Info("Postgres connected", zap.String("role", "write"))
	}

	readPool, err := newPool(ctx, readCfg)
	if err != nil {
		log.Error("Postgres connection failed", zap.String("role", "read"), zap.Error(err))
		return nil, fmt.Errorf("postgres %s: %w", "read", err)
	} else {
		log.Info("Postgres connected", zap.String("role", "read"))
	}

	return &DB{write: writePool, read: readPool, readTimeout: readCfg.Timeout, writeTimeout: writeCfg.Timeout, log: log}, nil
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
