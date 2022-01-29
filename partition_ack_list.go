package rmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

type PartitionACKListMQ struct {
	client *redis.Client // Redis客户端
}

func NewPartitionACKListMQ(client *redis.Client) *PartitionACKListMQ {
	return &PartitionACKListMQ{client: client}
}

func (q *PartitionACKListMQ) SendMsg(ctx context.Context, msg *Msg) error {
	return q.client.LPush(ctx, q.partitionTopic(msg.Topic, msg.Partition), msg.Body).Err()
}

// Consume 返回值代表消费过程中遇到的无法处理的错误
func (q *PartitionACKListMQ) Consume(ctx context.Context, topic string, partition int, h Handler) error {
	for {
		// 获取消息
		body, err := q.client.LIndex(ctx, q.partitionTopic(topic, partition), -1).Bytes()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		// 没有消息了，休眠一会
		if errors.Is(err, redis.Nil) {
			time.Sleep(time.Second)
			continue
		}
		// 处理消息
		err = h(&Msg{
			Topic:     topic,
			Body:      body,
			Partition: partition,
		})
		if err != nil {
			continue
		}
		// 如果处理成功，删除消息
		if err := q.client.RPop(ctx, q.partitionTopic(topic, partition)).Err(); err != nil {
			return err
		}
	}
}

func (q *PartitionACKListMQ) partitionTopic(topic string, partition int) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}
