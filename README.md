# GO Pulsar ElasticSearch Consumer


This is a **GO** demo application that showcases the following features:

- Integration with [Apache Pulsar](https://pulsar.apache.org/) by consuming messages from pulsar and indexing them in ElasticSearch.
- The use of **Avro** with Pulsar. 
- The use of [ElasticSearch](https://www.elastic.co/elasticsearch/) with GO. Specially how to use the bulk API for fast data ingestion.
- The use of Pulsar and custom [Prometheus Metrics](https://prometheus.io/)

This Pulsar Consumer listens for messages which are then batched indexed in ElasticSearch using the bulk API.

See Env Vars for more information.

Metrics are exposed on port **8002** path `/metrics`.

## Goals

- `make db`: Starts Pulsar and ElasticSearch.
- `make stop`: Stops Pulsar and ElasticSearch.
- `make build`: To build docker image.
- `make run`: Runs docker image.
- `make push`: Pushes image to sandbox docker registry.

## Schemas

### Pulsar Topic

Contains the data to be indexed into ElasticSearch 

```
{
  "name": "IngestionData",
  "type": "record",
  "namespace": "com.ramos",
  "fields": [
    {
      "name": "identifier",
      "type": "string"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "uuid",
      "type": "string"
    },
    {
      "name": "type",
      "type": "string"
    },
    {
      "name": "ingestion_time",
      "type": "long"
	  },
    {
      "name": "tags",
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "name": "Tags",
            "type": "record",
            "namespace": "com.ramos",
            "fields": [
              {
                "name": "type",
                "type": "string"
              },
              {
                "name": "value",
                "type": "string"
              }
            ]
          }
        }
      ],
      "default": null
    }
  ]
}
```

## ElasticSearch Mapping

```
{
  "settings": {
    "index.number_of_shards": %d,
    "index.number_of_replicas": %d,
    "index.refresh_interval": "%s"
  },
  "mappings": {
    "_source": {
      "enabled": true
    },
    "dynamic": "strict",
    "properties": {
      "type": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "identifier": {
        "type": "keyword"
      },
      "name": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword"
          }
        }
      },
      "ingestion_time": {
        "type": "date"
      },
      "persist_time": {
        "type": "date"
      },
      "uuid": {
        "type": "keyword"
      },
      "tags": {
        "type": "nested",
        "properties": {
          "type": {
            "type": "text"
          },
          "value": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword"
              }
            }
          }
        }
      }
    }
  }
}
```