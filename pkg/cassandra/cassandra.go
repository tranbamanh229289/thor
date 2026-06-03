package cassandra

import (
	"context"
	"fmt"
	"thor/pkg/config"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type DB struct {
	session *gocql.Session
	log     *zap.Logger
	cfg     *config.CassandraConfig
}

func (db *DB) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, db.cfg.Timeout)
	defer cancel()

	if err := db.session.Query("SELECT now() from system.local").WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandra ping : %w", err)
	}
	return nil
}

func (db *DB) Exec(ctx context.Context, stmt string, values ...any) error {
	if err := db.session.Query(stmt, values...).WithContext(ctx).Exec(); err != nil {
		db.log.Error("Cassandra exec failed:", zap.String("stmt", stmt), zap.Error(err))
		return fmt.Errorf("cassandra exe: %w", err)
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

	session, err := cluster.CreateSession()
	if err != nil {
		log.Error("Cassandra connection failed", zap.Error(err))
		return nil, fmt.Errorf("cassandra connect: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	if err := session.Query("SELECT now() from system.local").WithContext(ctx).Exec(); err != nil {
		session.Close()
		return nil, fmt.Errorf("cassandra ping : %w", err)
	}

	return &DB{session: session, log: log, cfg: cfg}, nil
}
