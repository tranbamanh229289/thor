package cassandra

import (
	"context"
	"fmt"
	"thor/pkg/config"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type IDB interface {
	Exec(ctx context.Context, stmt string, values ...any) error
	QueryRow(ctx context.Context, stmt string, dest []any, values ...any) error
	QueryMap(ctx context.Context, stmt string, values ...any) ([]map[string]any, error)
}

type DB struct {
	session *gocql.Session
	log     *zap.Logger
	cfg     *config.CassandraConfig
}

func (db *DB) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, db.cfg.Timeout)
	defer cancel()

	if err := db.session.Query("SELECT now() from system.local").WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandra ping: %w", err)
	}
	return nil
}

func (db *DB) Exec(ctx context.Context, stmt string, values ...any) error {
	if err := db.session.Query(stmt, values...).WithContext(ctx).Exec(); err != nil {
		db.log.Error("Cassandra exec failed:", zap.String("stmt", stmt), zap.Error(err))
		return fmt.Errorf("cassandra exec: %w", err)
	}
	return nil
}

func (db *DB) QueryRow(ctx context.Context, stmt string, dest []any, values ...any) error {
	if err := db.session.Query(stmt, values...).WithContext(ctx).Scan(dest...); err != nil {
		db.log.Error("Cassandra query row failed", zap.String("stmt", stmt), zap.Error(err))
		return fmt.Errorf("cassandra query row: %w", err)
	}
	return nil
}

func (db *DB) QueryMap(ctx context.Context, stmt string, values ...any) ([]map[string]any, error) {
	iter := db.session.Query(stmt, values...).WithContext(ctx).Iter()
	rows, err := iter.SliceMap()
	if err != nil {
		db.log.Error("cassandra query map failed", zap.String("stmt", stmt), zap.Error(err))
		return nil, fmt.Errorf("cassandra query map: %w", err)
	}
	return rows, nil
}

func (db *DB) Close() {
	if db.session != nil {
		db.session.Close()
	}
}

func New(ctx context.Context, cfg *config.CassandraConfig, log *zap.Logger) (*DB, error) {
	cluster := gocql.NewCluster(cfg.Hosts...)
	if cfg.Port > 0 {
		cluster.Port = cfg.Port
	}
	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	if cfg.NumConns > 0 {
		cluster.NumConns = cfg.NumConns
	}

	if cfg.Timeout > 0 {
		cluster.Timeout = cfg.Timeout
	}

	if cfg.Keyspace != "" {
		cluster.Keyspace = cfg.Keyspace
	}

	if cfg.Consistency != "" {
		cluster.Consistency = gocql.ParseConsistency(cfg.Consistency)
	}

	if cfg.LocalDC != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(cfg.LocalDC)
	}

	if cfg.Timeout > 0 {
		cluster.Timeout = cfg.Timeout
	}

	var err error
	var session *gocql.Session

	retries := cfg.Retries
	if retries == 0 {
		retries = 1
	}
	for attempt := 0; attempt < retries; attempt++ {
		session, err = cluster.CreateSession()
		if err == nil {
			log.Info("Cassandra connected")
			return &DB{session: session, log: log, cfg: cfg}, nil
		}

		log.Warn("Cassandra connect failed, retrying", zap.Int("attempt", attempt+1), zap.Error(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(cfg.RetryBackoffMs) * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("cassandra connect failed: %w", err)

}
