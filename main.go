package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"os"
	"runtime"
	"strconv"

	"github.com/MasterOfBinary/gobatch/batch"
	"github.com/MasterOfBinary/gobatch/source"
	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"ramos.com/go-pulsar-elasticsearch/pkg/es"
	"ramos.com/go-pulsar-elasticsearch/pkg/metrics"
	"ramos.com/go-pulsar-elasticsearch/pkg/model"
	"ramos.com/go-pulsar-elasticsearch/pkg/pulsar"

	log "github.com/sirupsen/logrus"
)

var ctx = context.Background()
var lastError *time.Time

// Implements Process() method to process each batch.
type dataBulkIndex struct {
	esService       *es.EsService
	batchingChannel chan interface{}
	indexingChannel chan *[]*model.IndexData
}

const (
	LogLevel        = "LOG_LEVEL"
	LogLevelDefault = "info"
)

func setEnvironment() {
	env := os.Getenv("ENV")

	e := godotenv.Load("/etc/" + env + ".env")
	if e != nil {
		e = godotenv.Load()
	}
	handleError(e, true)

	logLevel, _ := log.ParseLevel(getEnvWithDefault(LogLevel, LogLevelDefault))
	log.SetLevel(logLevel)
	log.SetFormatter(&log.JSONFormatter{})
	log.SetReportCaller(true)

	// concurrency level, use 2x number of vCPU
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	log.Debugf("GOMAXPROCS %v", runtime.GOMAXPROCS(-1))
}

func getEnvWithDefault(envKey, def string) string {
	val := os.Getenv(envKey)
	if val == "" {
		return def
	}
	return val
}

func handleError(err error, fatal ...bool) {
	if err != nil {
		log.Errorf("ERROR: %v", err)
		metrics.Error.Inc()
		t := time.Now()
		lastError = &t
		if len(fatal) > 0 && fatal[0] {
			panic(err)
		}
	}
}

func initHandlers(client *pulsar.PulsarService, es *es.EsService) {
	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler(client, es))
	r.HandleFunc("/ready", healthHandler(client, es))
	r.Handle("/metrics", promhttp.Handler())

	http.Handle("/", cors.AllowAll().Handler(r))
}

func initBatchProcessing(esService *es.EsService, channelSize int) (context.CancelFunc, *dataBulkIndex) {
	maxItems, _ := strconv.Atoi(os.Getenv("MAX_BATCH_SIZE"))
	minTime, _ := strconv.Atoi(os.Getenv("BATCH_MIN_TIME"))
	maxTime, _ := strconv.Atoi(os.Getenv("BATCH_MAX_TIME"))
	minItems, _ := strconv.Atoi(os.Getenv("MIN_BATCH_SIZE"))

	config := batch.NewConstantConfig(&batch.ConfigValues{
		MinTime:  time.Duration(minTime) * time.Second,
		MaxTime:  time.Duration(maxTime) * time.Second,
		MinItems: uint64(minItems),
		MaxItems: uint64(maxItems),
	})

	log.Infof("Batch minTime: %v", minTime)

	batch := batch.New(config)
	// Channel is a Source that reads from a channel until it's closed
	batchingChannel := make(chan interface{}, channelSize)
	indexingChannel := make(chan *[]*model.IndexData, channelSize)

	source := source.Channel{
		Input: batchingChannel,
	}

	bulkIndex := &dataBulkIndex{
		esService:       esService,
		indexingChannel: indexingChannel,
		batchingChannel: batchingChannel,
	}

	// Go runs in the background while the main goroutine processes errors
	ctx, batchCancel := context.WithCancel(ctx)

	_ = batch.Go(ctx, &source, bulkIndex)

	return batchCancel, bulkIndex
}

func (c dataBulkIndex) receiveMessage(channel chan pulsarclient.ConsumerMessage) {
	for msg := range channel {
		metrics.DataReceived.Inc()
		log.Debugf("receiveMessage: Got Message: %v", msg)
		data, err := parseMsg(msg)
		if err != nil {
			log.Errorf("receiveMessage: Error parsing message: %v. ERROR: %v", msg, err)
			handleError(err)
		} else {
			c.batchingChannel <- data
		}
	}
}

func parseMsg(msg pulsarclient.ConsumerMessage) (*model.IndexData, error) {
	log.Debugf("Got Message: %v", msg)
	var data model.IngestionData
	err := msg.GetSchemaValue(&data)
	log.Debugf("data: %v", data)
	if err != nil {
		return nil, err
	}

	IndexData := data.ToIndex()
	IndexData.Message = msg

	return &IndexData, nil
}

func (c dataBulkIndex) Process(ctx context.Context, ps *batch.PipelineStage) {
	// Process needs to close ps after it's done
	defer ps.Close()

	var msgs []*model.IndexData
	for item := range ps.Input {
		data := item.Get().(*model.IndexData)
		msgs = append(msgs, data)
	}

	c.indexingChannel <- &msgs
}

func (c dataBulkIndex) bulkIndexProcess(pulsar *pulsar.PulsarService) {

	for items := range c.indexingChannel {
		log.Debugf("Batch Received!")
		start := time.Now()
		ids, err := (*c.esService).BulkIndex(items)
		if err != nil {
			log.Errorf("bulkIndexProcess: ERROR: %v", err)
			handleError(err)
		}

		found := false
		for _, a := range *items {
			for _, id := range ids {
				if a.Uuid == id {
					found = true
					(*a).Message.Ack(a.Message)
					metrics.DataIngested.Inc()
					break
				}
			}
			if !found {
				(*pulsar).NAck(a.Message)
			}
		}
		elapsed := time.Since(start)
		log.Infof("Index Batch %d processed in %v", len(*items), elapsed)
	}

}

func main() {
	log.Info("Starting Index Data Consumer...")

	setEnvironment()

	size, _ := strconv.Atoi(os.Getenv("CHANNEL_SIZE"))
	retries, _ := strconv.Atoi(os.Getenv("RETRIES"))
	queueSize, _ := strconv.Atoi(os.Getenv("PULSAR_QUEUE_SIZE"))
	workers, _ := strconv.Atoi(os.Getenv("NUMBER_INDEX_BULK_WORKERS"))
	shards, _ := strconv.Atoi(os.Getenv("ES_NUMBER_SHARDS"))
	replicas, _ := strconv.Atoi(os.Getenv("ES_NUMBER_REPLICAS"))

	indexFile, err := model.ReadSchema(os.Getenv("INDEX_SCHEMA"))
	if err != nil {
		handleError(err, true)
	}

	index := os.Getenv("INDEX")

	log.Infof("ES Index set to %d", index)

	esParams := &es.EsOptions{
		Connection:      os.Getenv("ES_URL"),
		Retries:         retries,
		Workers:         workers,
		RefreshInterval: os.Getenv("ES_REFRESH_INTERVAL"),
		Shards:          shards,
		Replicas:        replicas,
		Username:        os.Getenv("ES_USERNAME"),
		Password:        os.Getenv("ES_PASSWORD"),
		SchemaFile:      &indexFile,
		Index:           index,
	}

	log.Infof("Initializing ES with options: %v", esParams)
	esService, err := es.NewEsService(esParams)
	if err != nil {
		handleError(err, true)
	}

	schema, err := model.ReadSchema(os.Getenv("DATA_SCHEMA"))
	if err != nil {
		handleError(err, true)
	}

	delay, _ := strconv.Atoi(os.Getenv("INSERT_RETRY_DELAY"))
	dataConsumerChannel := make(chan pulsarclient.ConsumerMessage, size)
	pulsarParams := &pulsar.PulsarOptions{
		Connection:     os.Getenv("PULSAR_URL"),
		Topic:          os.Getenv("DATA_TOPIC"),
		DlqTopic:       os.Getenv("DATA_DLQ_TOPIC"),
		Subscription:   os.Getenv("SUBSCRIPTION_NAME"),
		Retries:        retries,
		Schema:         &schema,
		QueueSize:      queueSize,
		RetryDelay:     delay,
		ReceiveChannel: &dataConsumerChannel,
	}

	log.Infof("Initializing Pulsar client with options: %v", pulsarParams)

	pulsarClient, err := pulsar.NewPulsarClient(pulsarParams)
	if err != nil {
		handleError(err, true)
	}

	log.Info("Pulsar and ES initialized, initializing batch processing...")

	batchCancel, bulKIndexer := initBatchProcessing(&esService, size)

	consumeThreads, _ := strconv.Atoi(os.Getenv("NUMBER_CONSUME_THREADS"))
	for threarNr := int64(0); threarNr < int64(consumeThreads); threarNr++ {
		go bulKIndexer.receiveMessage(dataConsumerChannel)
	}

	indexThreads, _ := strconv.Atoi(os.Getenv("NUMBER_INDEX_THREADS"))
	for threarNr := int64(0); threarNr < int64(indexThreads); threarNr++ {
		go bulKIndexer.bulkIndexProcess(&pulsarClient)
	}

	log.Infof("Initializing HTTP Service for metrics")
	initHandlers(&pulsarClient, &esService)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8002"
	}

	srv := &http.Server{
		Addr: ":" + port,
	}

	log.Info("Initialization completed, starting server...")

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			handleError(err, true)
		}
	}()

	log.Infof("Server Started listening on port %v", port)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-done
	log.Info("Server Stopped")

	ctx, httpCancel := context.WithTimeout(ctx, 5*time.Second)
	defer func() {
		log.Info("Cancelling Pulsar and HTTP")
		close(dataConsumerChannel)
		pulsarClient.Close()
		batchCancel()
		httpCancel()
	}()

	if err := srv.Shutdown(ctx); err != nil {
		log.Errorf("Server Shutdown Failed:%+v", err)
	}

}

func healthHandler(pulsar *pulsar.PulsarService, es *es.EsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := (*pulsar).HealthCheck()
		if !handleHTTPError(err, &w) {

			esReply, err := (*es).HealthCheck()

			if !handleHTTPError(err, &w) {

				goVer := runtime.Version()
				numRoutines := runtime.NumGoroutine()

				lastErrorStr := ""
				if lastError != nil {
					lastErrorStr = string(lastError.String())
				}

				result := model.HealthStatus{
					AppName:          "go-pulsar-elasticsearch",
					Status:           "UP",
					GoVersion:        goVer,
					NumberOfRoutines: numRoutines,
					LastError:        lastErrorStr,
					Info:             esReply,
				}

				w.Header().Set("Content-Type", "application/json")
				err := json.NewEncoder(w).Encode(result)
				if err != nil {
					log.Errorf("Error encoding %v", result)
					handleHTTPError(err, &w)
				}
			}

		}
	}

}

func handleHTTPError(e error, w *http.ResponseWriter) bool {
	if e != nil {
		handleError(e, true)
		return setError(e, w) == nil
	}

	return false
}

func setError(e error, w *http.ResponseWriter) error {
	(*w).Header().Set("content-type", "application/json")
	(*w).WriteHeader(http.StatusInternalServerError)
	_, e = (*w).Write([]byte(`{"error": "` + e.Error() + `"}`))
	return e
}
