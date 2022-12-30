package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Error
var Error = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "go_pulsar_es",
	Name:      "es_consumer_error",
	Help:      "Error Indexing Data",
})

// IndexError
var IndexError = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "go_pulsar_es",
	Name:      "es_consumer_index_error",
	Help:      "Error Indexing Data Error",
})

// ErrorDLQ
var ErrorDLQ = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "go_pulsar_es",
	Name:      "es_consumer_error_dlq",
	Help:      "Fatal Error Data sent to DLQ",
})

// DataReceived
var DataReceived = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "go_pulsar_es",
	Name:      "es_consumer_data_received",
	Help:      "Data Received",
})

// DataIndexed
var DataIngested = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "go_pulsar_es",
	Name:      "es_consumer_data_indexed",
	Help:      "Data Indexed",
})
