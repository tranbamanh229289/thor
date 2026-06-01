package cassandra

import (
	"context"
	"fmt"
	"thor/pkg/config"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

type DB struct {
	session *gocql.Session
	log     *zap.Logger
	timeout time.Duration
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

	return &DB{session: session, log: log, timeout: cfg.Timeout}, nil
}

func (db *DB) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, db.timeout)
	defer cancel()

	if err := db.session.Query("SELECT now() from system.local").WithContext(ctx).Exec(); err != nil {
		db.session.Close()
		return fmt.Errorf("cassandra ping : %w", err)
	}
	return nil
}

func (db *DB) Close() {
	if db.session != nil {
		db.session.Close()
	}
}

func getAddress() {

}
