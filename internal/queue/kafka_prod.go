package queue

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaProducer struct {
	broker     string
	topic      string
	client     *kgo.Client
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	connected  bool
}

func NewKafkaProducer(broker, topic string) *KafkaProducer {
	return &KafkaProducer{
		broker: broker,
		topic:  topic,
	}
}

// Start initializes the Franz Kafka client
func (p *KafkaProducer) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.connected {
		return nil
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(p.broker),
		kgo.DefaultProduceTopic(p.topic),
		// Enable LZ4 compression as specified in optimization requirements
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		// High performance tuning
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Ping the cluster to make sure we can connect
	pCtx, pCancel := context.WithTimeout(ctx, 3*time.Second)
	defer pCancel()
	if err := client.Ping(pCtx); err != nil {
		client.Close()
		return fmt.Errorf("failed to connect to Kafka broker at %s: %w", p.broker, err)
	}

	p.client = client
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.connected = true
	fmt.Printf("Successfully connected to Kafka broker at %s, topic: %s\n", p.broker, p.topic)
	return nil
}

// Publish sends a telemetry message to Kafka.
// The key is set to the SessionUID to guarantee chronological order within the same partition.
func (p *KafkaProducer) Publish(sessionUID uint64, packetId uint8, payload interface{}) error {
	p.mu.Lock()
	client := p.client
	connected := p.connected
	p.mu.Unlock()

	if !connected || client == nil {
		return fmt.Errorf("kafka producer is not connected")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to serialize telemetry payload: %w", err)
	}

	// Create partition key based on SessionUID
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sessionUID)

	record := &kgo.Record{
		Topic: p.topic,
		Key:   key,
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: "packet_id", Value: []byte{packetId}},
			{Key: "session_uid", Value: key},
		},
	}

	// Asynchronous produce for high throughput
	p.client.Produce(p.ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("Failed to deliver message to Kafka: %v\n", err)
		}
	})

	return nil
}

// Stop closes the Kafka client connection
func (p *KafkaProducer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.connected {
		return
	}

	if p.cancel != nil {
		p.cancel()
	}

	if p.client != nil {
		p.client.Close()
	}
	p.connected = false
}
