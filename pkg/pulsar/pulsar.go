package pulsar

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

var ctx = context.Background()

type PulsarClient struct {
	client   *pulsar.Client
	consumer *pulsar.Consumer
	topic    string
}

type PulsarService interface {
	Close() error
	Ack(msg pulsar.ConsumerMessage)
	NAck(msg pulsar.ConsumerMessage)
	HealthCheck() error
}

type PulsarOptions struct {
	Connection     string
	Topic          string
	Subscription   string
	DlqTopic       string
	Retries        int
	Schema         *string
	QueueSize      int
	RetryDelay     int
	ReceiveChannel *chan pulsar.ConsumerMessage
}

// NewPulsarClient Creates new Service
func NewPulsarClient(ops *PulsarOptions) (PulsarService, error) {

	log.Infof("NewPulsarClient, connection %s", ops.Connection)

	client, consumer, e := connectPulsarWithRetry(ops, 5*time.Second)
	if e != nil {
		return nil, e
	}

	_, e = (*client).TopicPartitions(ops.Topic) // health check

	if e != nil {
		return nil, e
	}

	c := &PulsarClient{
		client:   client,
		consumer: consumer,
		topic:    ops.Topic,
	}

	return c, nil

}

func connectPulsarWithRetry(ops *PulsarOptions, sleep time.Duration) (c *pulsar.Client, o *pulsar.Consumer, err error) {
	for i := 0; i < ops.Retries; i++ {
		if i > 0 {
			log.Warnf("retrying after error: %v, Attempt: %v", err, i)
			time.Sleep(sleep)
			sleep *= 2
		}
		c, p, err := connectPulsar(ops)
		if err == nil {
			return &c, &p, nil
		}
	}
	return nil, nil, fmt.Errorf("after %d attempts, last error: %s", ops.Retries, err)
}

func connectPulsar(ops *PulsarOptions) (pulsar.Client, pulsar.Consumer, error) {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     ops.Connection,
		OperationTimeout:        60 * time.Second,
		ConnectionTimeout:       20 * time.Second,
		MaxConnectionsPerBroker: 10,
	})

	avroSchema := pulsar.NewAvroSchema(*ops.Schema, nil)

	if err == nil {
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:               ops.Topic,
			SubscriptionName:    ops.Subscription,
			Type:                pulsar.Shared,
			NackRedeliveryDelay: time.Duration(ops.RetryDelay) * time.Second,
			DLQ: &pulsar.DLQPolicy{
				MaxDeliveries:   uint32(ops.Retries),
				DeadLetterTopic: ops.DlqTopic,
			},
			RetryEnable:                    true,
			Schema:                         avroSchema,
			MessageChannel:                 *ops.ReceiveChannel,
			EnableDefaultNackBackoffPolicy: false,
		})
		return client, consumer, err
	}

	return nil, nil, err

}

func (c *PulsarClient) HealthCheck() error {
	_, e := (*c.client).TopicPartitions(c.topic)
	return e
}

func (c *PulsarClient) Ack(msg pulsar.ConsumerMessage) {
	(*c.consumer).Ack(msg.Message)

}

func (c *PulsarClient) NAck(msg pulsar.ConsumerMessage) {
	(*c.consumer).Nack(msg.Message)
}

// Close connections
func (c *PulsarClient) Close() error {
	if c.client != nil {
		(*c.client).Close()
	}
	if c.consumer != nil {
		(*c.client).Close()
	}
	return nil
}
