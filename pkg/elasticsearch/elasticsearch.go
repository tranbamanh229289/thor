package elasticsearch

import (
	"context"
	"fmt"
	"thor/pkg/config"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/typedapi/core/get"
	"github.com/elastic/go-elasticsearch/v9/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v9/typedapi/types"
	"go.uber.org/zap"
)

type SearchEngine struct {
	client *elasticsearch.TypedClient
	log    *zap.Logger
	cfg    *config.ElasticsearchConfig
}

type BulkIndex struct {
	ID       string
	Action   string
	Document any
}

func (s *SearchEngine) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	res, err := s.client.Cluster.Health().Do(ctx)

	if err != nil {
		return fmt.Errorf("elasticsearch health: %w", err)
	}
	switch res.Status.Name {
	case "red":
		return fmt.Errorf("elasticsearch cluster status: %s", res.Status.Name)
	case "yellow":
		s.log.Warn("elasticsearch cluster status", zap.String("Status", res.Status.Name))
	}

	return nil

}

func (s *SearchEngine) Close(ctx context.Context) error {
	if s.client != nil {
		return s.client.Close(ctx)
	}
	return nil
}

func (s *SearchEngine) CreateIndex(ctx context.Context, index string, mapping *types.TypeMapping) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Indices.Create(index)
	if mapping != nil {
		req.Mappings(mapping)
	}
	if _, err := req.Do(ctx); err != nil {
		s.log.Error("Elasticsearch create index failed", zap.String("index", index), zap.Error(err))
		return fmt.Errorf("elasticsearch create index %s: %w", index, err)
	}
	return nil
}

func (s *SearchEngine) DeleteIndex(ctx context.Context, index string) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Indices.Delete(index)
	if _, err := req.Do(ctx); err != nil {
		s.log.Error("Elasticsearch delete index failed", zap.String("index", index), zap.Error(err))
		return fmt.Errorf("elasticsearch delete index %s:%w ", index, err)
	}
	return nil
}

func (s *SearchEngine) Index(ctx context.Context, index, docID string, doc any) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Index(index).Document(doc)
	if docID != "" {
		req.Id(docID)
	}
	if _, err := req.Do(ctx); err != nil {
		s.log.Error("Elasticsearch index failed", zap.String("index", index), zap.String("id", docID), zap.Error(err))
		return fmt.Errorf("elasticsearch index %s:%s:%w", index, docID, err)
	}
	return nil
}

func (s *SearchEngine) Update(ctx context.Context, index, docID string, partialDoc any) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Update(index, docID).Doc(partialDoc)
	if _, err := req.Do(ctx); err != nil {
		s.log.Error("Elasticsearch update failed", zap.String("index", index), zap.String("id", docID), zap.Error(err))
		return fmt.Errorf("elasticsearch update %s:%s:%w", index, docID, err)
	}
	return nil
}

func (s *SearchEngine) Delete(ctx context.Context, index, docID string) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Delete(index, docID)
	if _, err := req.Do(ctx); err != nil {
		s.log.Error("Elasticsearch delete failed", zap.String("index", index), zap.String("id", docID), zap.Error(err))
		return fmt.Errorf("elasticsearch delete %s: %w", index, err)
	}
	return nil
}

func (s *SearchEngine) Get(ctx context.Context, index, docID string) (*get.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Get(index, docID)
	res, err := req.Do(ctx)
	if err != nil {
		s.log.Error("Elasticsearch get failed", zap.String("index", index), zap.String("id", docID), zap.Error(err))
		return nil, fmt.Errorf("elasticsearch get %s:%w", index, err)
	}
	return res, nil
}

func (s *SearchEngine) BulkIndex(ctx context.Context, index string, items []BulkIndex) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	req := s.client.Bulk().Index(index)

	for _, item := range items {
		action := item.Action
		if action == "" {
			action = "index"
		}

		switch action {
		case "index":
			op := types.NewIndexOperation()
			if item.ID != "" {
				op.Id_ = &item.ID

			}
			req.IndexOp(*op, item.Document)
		case "create":
			op := types.NewCreateOperation()
			if item.ID != "" {
				op.Id_ = &item.ID

			}
			req.CreateOp(*op, item.Document)
		case "delete":
			op := types.NewDeleteOperation()
			if item.ID != "" {
				op.Id_ = &item.ID
				req.DeleteOp(*op)
			}
		case "update":
			op := types.NewUpdateOperation()
			if item.ID != "" {
				op.Id_ = &item.ID
				req.UpdateOp(*op, item.Document, &types.UpdateAction{})
			}
		}
	}
	res, err := req.Do(ctx)
	if err != nil {
		s.log.Error("Elasticsearch bulk execution failed", zap.String("index", index), zap.Error(err))
		return fmt.Errorf("elasticsearch bulk  %s:%w", index, err)
	}
	if res.Errors {
		return fmt.Errorf("Elasticsearch bulk execution failed")
	}

	return nil
}

func (s *SearchEngine) Search(ctx context.Context, index string, query *types.Query, from, size *int) (*search.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Search().Index(index)
	if query != nil {
		req.Query(query)
	}
	if from != nil {
		req.From(*from)
	}
	if size != nil {
		req.Size(*size)
	}

	res, err := req.Do(ctx)
	if err != nil {
		s.log.Error("Elasticsearch search failed", zap.String("index", index), zap.Error(err))
		return nil, fmt.Errorf("elasticsearch search %s:%w", index, err)
	}
	return res, nil
}

func (s *SearchEngine) Count(ctx context.Context, index string, query *types.Query) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Count().Index(index)
	if query != nil {
		req.Query(query)
	}

	res, err := req.Do(ctx)
	if err != nil {
		s.log.Error("Elasticsearch count failed", zap.String("index", index), zap.Error(err))
		return 0, fmt.Errorf("elasticsearch count %s: %w", index, err)
	}
	return res.Count, nil
}

func (s *SearchEngine) Aggregate(ctx context.Context, index string, query *types.Query, aggs map[string]types.Aggregations) (*search.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req := s.client.Search().Index(index).Size(0) // Thường lấy stats thì set size = 0 để tăng tốc
	if query != nil {
		req.Query(query)
	}
	if aggs != nil {
		req.Aggregations(aggs)
	}

	res, err := req.Do(ctx)
	if err != nil {
		s.log.Error("Elasticsearch aggregation failed", zap.String("index", index), zap.Error(err))
		return nil, fmt.Errorf("elasticsearch aggregation %s: %w", index, err)
	}
	return res, nil
}

func New(ctx context.Context, cfg *config.ElasticsearchConfig, log *zap.Logger) (*SearchEngine, error) {
	var client *elasticsearch.TypedClient
	var err error

	if client, err = elasticsearch.NewTyped(elasticsearch.WithAddresses(cfg.Addresses...), elasticsearch.WithBasicAuth(cfg.Username, cfg.Password)); err != nil {
		log.Error("Elasticsearch client init failed", zap.Error(err))
		return nil, fmt.Errorf("elasticsearch client: %w", err)
	}

	retries := cfg.Retries
	if retries <= 0 {
		retries = 1
	}

	for attempt := 0; attempt < retries; attempt++ {
		res, err := client.Cluster.Health().Do(ctx)
		if err == nil && res.Status.Name != "red" {
			log.Info("Elasticsearch connected")
			return &SearchEngine{
				client: client,
				log:    log,
				cfg:    cfg,
			}, nil

		}
		log.Warn("Elasticsearch connect failed, retrying", zap.Int("attempt", attempt+1), zap.Error(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(cfg.RetryBackoffMs) * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("elasticsearch health: %w", err)

}
