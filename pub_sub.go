package rmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"strings"
)

type PubSubMQ struct {
	client *redis.Client // Redis客户端
}

func NewPubSubMQ(client *redis.Client) *PubSubMQ {
	return &PubSubMQ{client: client}
}

func (q *PubSubMQ) SendMsg(ctx context.Context, msg *Msg) error {
	return q.client.Publish(ctx, q.partitionTopic(msg.Topic, msg.Partition), msg.Body).Err()
}

// Consume 返回值代表消费过程中遇到的无法处理的错误
func (q *PubSubMQ) Consume(ctx context.Context, topic string, partition int, h Handler) error {
	// 订阅频道
	channel := q.client.Subscribe(ctx, q.partitionTopic(topic, partition)).Channel()
	for msg := range channel {
		// 处理消息
		h(&Msg{
			Topic:     topic,
			Body:      []byte(msg.Payload),
			Partition: partition,
		})
	}
	return errors.New("channel closed")
}

// ConsumeMultiPartitions 返回值代表消费过程中遇到的无法处理的错误
func (q *PubSubMQ) ConsumeMultiPartitions(ctx context.Context, topic string, partitions []int, h Handler) error {
	// 订阅频道
	channels := make([]string, len(partitions))
	for i, partition := range partitions {
		channels[i] = q.partitionTopic(topic, partition)
	}
	channel := q.client.Subscribe(ctx, channels...).Channel()
	for msg := range channel {
		// 处理消息
		_, partitionString, _ := strings.Cut(msg.Channel, ":")
		partition, _ := strconv.Atoi(partitionString)
		h(&Msg{
			Topic:     topic,
			Body:      []byte(msg.Payload),
			Partition: partition,
		})
	}
	return errors.New("channels closed")
}

func (q *PubSubMQ) partitionTopic(topic string, partition int) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}
