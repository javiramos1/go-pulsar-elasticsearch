package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	log "github.com/sirupsen/logrus"
	"ramos.com/go-pulsar-elasticsearch/pkg/metrics"
	"ramos.com/go-pulsar-elasticsearch/pkg/model"
)

var ctx = context.Background()

type EsClient struct {
	client  *elasticsearch.Client
	workers int
	index   string
}

type EsService interface {
	BulkIndex(items *[]*model.IndexData) ([]string, error)
	HealthCheck() (string, error)
}

type EsOptions struct {
	Connection      string
	Retries         int
	Workers         int
	Shards          int
	RefreshInterval string
	Replicas        int
	Username        string
	Password        string
	SchemaFile      *string
	Index           string
}

// NewEsService Creates new Service
func NewEsService(ops *EsOptions) (EsService, error) {

	log.Infof("NewEsService, connection %s", ops.Connection)

	client, e := connectEsWithRetry(ops, 5*time.Second)
	if e != nil {
		return nil, e
	}

	_, e = client.Ping()
	if e != nil {
		return nil, e
	}

	log.Info("NewEsService, creating index...")

	e = createIndex(ops.Index, client, ops)
	if e != nil {
		return nil, e
	}

	c := &EsClient{
		client:  client,
		workers: ops.Workers,
		index:   ops.Index,
	}

	return c, nil

}

func createIndex(aliasName string, c *elasticsearch.Client, ops *EsOptions) error {
	currentTime := time.Now()
	date := currentTime.Format("2006-01-02")
	indexName := aliasName + "_" + date
	log.Infof("Creating Index: %s", indexName)
	body := fmt.Sprintf(*ops.SchemaFile, ops.Shards, ops.Replicas, ops.RefreshInterval)
	log.Debugf("Creating Index with Body: %s", body)
	res, err := c.Indices.Create(indexName, func(val *esapi.IndicesCreateRequest) {
		(*val).Body = strings.NewReader(body)
	})
	defer res.Body.Close()
	if err != nil {
		return fmt.Errorf("Cannot create index: %s", err)
	}
	if res.IsError() {
		if !strings.Contains(res.String(), "resource_already_exists_exception") {
			return fmt.Errorf("Cannot create index: %s", res.String())
		}

	}

	return addAlias(indexName, aliasName, c)
}

func addAlias(indexName string, aliasName string, c *elasticsearch.Client) error {

	res, err := c.Indices.PutAlias([]string{indexName}, aliasName)
	defer res.Body.Close()
	if err != nil {
		return fmt.Errorf("Cannot create alias: %s", err)
	}
	if res.IsError() {
		if !strings.Contains(res.String(), "resource_already_exists_exception") {
			return fmt.Errorf("Cannot create alias: %s", res.String())
		}

	}
	return nil
}

func connectEsWithRetry(ops *EsOptions, sleep time.Duration) (e *elasticsearch.Client, err error) {
	for i := 0; i < ops.Retries; i++ {
		if i > 0 {
			log.Warnf("retrying after error: %v, Attempt: %v", err, i)
			time.Sleep(sleep)
			sleep *= 2
		}
		c, err := connectEs(ops)
		if err == nil {
			return c, nil
		}
	}
	return nil, fmt.Errorf("after %d attempts, last error: %s", ops.Retries, err)
}

func connectEs(ops *EsOptions) (*elasticsearch.Client, error) {

	cfg := elasticsearch.Config{
		Addresses:           []string{ops.Connection},
		MaxRetries:          ops.Retries,
		CompressRequestBody: true,
		RetryOnStatus:       []int{429, 502, 503, 504, 500},
		RetryBackoff: func(i int) time.Duration {
			d := time.Duration(math.Exp2(float64(i))) * time.Second
			log.Warnf("Attempt: %d | Sleeping for %s...\n", i, d)
			return d
		},
	}

	log.Infof("ES Username: %s", ops.Username)
	if ops.Username != "" {
		cfg.Username = ops.Username
	}
	if ops.Password != "" {
		cfg.Password = ops.Password
	}

	log.Debugf("ES Conf: %v", cfg)
	return elasticsearch.NewClient(cfg)

}

func (c *EsClient) BulkIndex(items *[]*model.IndexData) ([]string, error) {
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         c.index,
		NumWorkers:    c.workers,
		Client:        c.client,
		Timeout:       5 * time.Minute,
		FlushBytes:    1 * 5000000,
		FlushInterval: 30 * time.Second,
	})
	if err != nil {
		log.Errorf("Error creating the bulk indexer: %s", err)
		return nil, err
	}
	retries := 50
	ids := []string{}
	for _, a := range *items {
		b, e := json.Marshal(&a)
		if err != nil {
			log.Errorf("Error parsing item json: %s", err)
			return nil, e
		}
		err := bi.Add(
			ctx,
			esutil.BulkIndexerItem{
				Action:          "index",
				DocumentID:      a.Uuid,
				RetryOnConflict: &retries,
				Body:            bytes.NewReader(b),
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					ids = append(ids, item.DocumentID)
				},
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					var bytes []byte
					item.Body.Read(bytes)

					log.Errorf("BulkIndex: item ERROR: ", res.Error.Type, res.Error.Reason, item.DocumentID, err, string(bytes))
					metrics.IndexError.Inc()
				},
			},
		)
		if err != nil {
			log.Errorf("BulkIndex: Error Adding element to bulk: %s", err)
			return nil, err
		}
	}

	err = bi.Close(ctx)
	if err != nil {
		log.Errorf("BulkIndex: Error: %s", err)
		return nil, err
	}

	return ids, nil
}

func (c *EsClient) HealthCheck() (string, error) {
	ret, e := (*c.client).Info()
	return fmt.Sprintf("%v", ret), e
}
