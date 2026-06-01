package elasticsearch

import (
	"context"
	"fmt"
	"thor/pkg/config"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
)

type SearchEngine struct {
	client  *elasticsearch.Client
	log     *zap.Logger
	timeout time.Duration
}

func New(ctx context.Context, cfg *config.ElasticsearchConfig, log *zap.Logger) (*SearchEngine, error) {
	esCfg := elasticsearch.Config{
		Addresses: cfg.Address,
		Username:  cfg.Username,
		Password:  cfg.Password,
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		log.Error("Elasticsearch client init failed", zap.Error(err))
		return nil, fmt.Errorf("elasticsearch client: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	res, err := client.Cluster.Health(client.Cluster.Health.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("elasticsearch health: %w", err)
	}
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch health: %w", res.Status())
	}

	defer res.Body.Close()

	return &SearchEngine{
		client:  client,
		log:     log,
		timeout: cfg.Timeout,
	}, nil
}

func (s *SearchEngine) HealCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.client.Cluster.Health(s.client.Cluster.Health.WithContext(ctx))

	if err != nil {
		return fmt.Errorf("elasticsearch health: %w", err)
	}
	defer res.Body.Close()
	return nil

}
