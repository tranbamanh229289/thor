package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"thor/pkg/config"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"go.uber.org/zap"
)

type SearchEngine struct {
	client *elasticsearch.Client
	log    *zap.Logger
	cfg    *config.ElasticsearchConfig
}

func (s *SearchEngine) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	res, err := s.client.Cluster.Health(s.client.Cluster.Health.WithContext(ctx))

	if err != nil {
		return fmt.Errorf("elasticsearch health: %w", err)
	}
	defer res.Body.Close()
	return nil

}

func (s *SearchEngine) Close(ctx context.Context) error {
	if s.client != nil {
		return s.client.Close(ctx)
	}
	return nil
}

func (s *SearchEngine) Index(ctx context.Context, index, docID string, doc any) error {
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("elasticsearch index marshal: %w", err)
	}

	opts := []func(*esapi.IndexRequest){
		s.client.Index.WithContext(ctx),
		s.client.Index.WithDocumentID(docID),
	}

}

func (s *SearchEngine) Search(ctx context.Context) error {

}

func New(ctx context.Context, cfg *config.ElasticsearchConfig, log *zap.Logger) (*SearchEngine, error) {
	esCfg := elasticsearch.Config{
		Addresses: cfg.Addresses,
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

	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch health: %s", res.Status())
	}

	return &SearchEngine{
		client: client,
		log:    log,
		cfg:    cfg,
	}, nil
}
