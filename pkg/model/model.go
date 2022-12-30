package model

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	pulsarModel "github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

type Tag struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type IngestionData struct {
	Tags          map[string][]Tag `json:"tags,omitempty"`
	Uuid          string           `json:"uuid"`
	Identifier    string           `json:"identifier"`
	Name          string           `json:"name"`
	Type          string           `json:"type"`
	IngestionTime int64            `json:"ingestion_time"`
	Retries       int              `json:"-"`
}

func (a IngestionData) ToIndex() IndexData {

	now := time.Now()
	ingestTime := time.UnixMilli(a.IngestionTime)

	ret := IndexData{
		Identifier:    a.Identifier,
		Uuid:          a.Uuid,
		Name:          a.Name,
		Type:          a.Type,
		IngestionTime: &ingestTime,
		PersistTime:   &now,
		Tags:          a.Tags["array"],
	}

	return ret
}

type IndexData struct {
	Tags          []Tag                       `json:"tags,omitempty"`
	Uuid          string                      `json:"uuid"`
	Identifier    string                      `json:"identifier"`
	Name          string                      `json:"name"`
	Type          string                      `json:"type"`
	IngestionTime *time.Time                  `json:"ingestion_time,omitempty"`
	PersistTime   *time.Time                  `json:"persist_time,omitempty"`
	Message       pulsarModel.ConsumerMessage `json:"-"`
}

func ReadSchema(file string) (string, error) {
	absPath, _ := filepath.Abs(file)
	log.Infof("Reading file: %s", absPath)
	schema, err := ioutil.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("Error reading file %s: %v", file, err)
	}

	return string(schema), nil
}

// HealthStatus response
type HealthStatus struct {
	AppName          string `json:"app_name"`
	Status           string `json:"status"`
	GoVersion        string `json:"go_version"`
	NumberOfRoutines int    `json:"number_of_routines"`
	LastError        string `json:"last_error"`
	Info             string `json:"info"`
}
