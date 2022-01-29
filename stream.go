package rmq

import (
	"context"
	"github.com/go-redis/redis/v8"
)

var errBusyGroup = "BUSYGROUP Consumer Group name already exists"

type StreamMQ struct {
	// Redis客户端
	client *redis.Client
	// 最大消息数量，如果大于这个数量，旧消息会被删除，0表示不管
	maxLen int64
	// Approx是配合MaxLen使用的，表示几乎精确的删除消息，也就是不完全精确，由于stream内部是流，所以设置此参数xadd会更加高效
	approx bool
}

func NewStreamMQ(client *redis.Client, maxLen int, approx bool) *StreamMQ {
	return &StreamMQ{
		client: client,
		maxLen: int64(maxLen),
		approx: approx,
	}
}

func (q *StreamMQ) SendMsg(ctx context.Context, msg *Msg) error {
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: msg.Topic,
		MaxLen: q.maxLen,
		Approx: q.approx,
		ID:     "*",
		Values: []interface{}{"body", msg.Body},
	}).Err()
}

// Consume 返回值代表消费过程中遇到的无法处理的错误
// group 消费者组
// consumer 消费者组里的消费者
// batchSize 每次批量获取一批的大小
// start 用于创建消费者组的时候指定起始消费ID，0表示从头开始消费，$表示从最后一条消息开始消费
func (q *StreamMQ) Consume(ctx context.Context, topic, group, consumer, start string, batchSize int, h Handler) error {
	err := q.client.XGroupCreateMkStream(ctx, topic, group, start).Err()
	if err != nil && err.Error() != errBusyGroup {
		return err
	}
	for {
		// 拉取新消息
		if err := q.consume(ctx, topic, group, consumer, ">", batchSize, h); err != nil {
			return err
		}
		// 拉取已经投递却未被ACK的消息，保证消息至少被成功消费1次
		if err := q.consume(ctx, topic, group, consumer, "0", batchSize, h); err != nil {
			return err
		}
	}
}

func (q *StreamMQ) consume(ctx context.Context, topic, group, consumer, id string, batchSize int, h Handler) error {
	// 阻塞的获取消息
	result, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{topic, id},
		Count:    int64(batchSize),
	}).Result()
	if err != nil {
		return err
	}
	// 处理消息
	for _, msg := range result[0].Messages {
		err := h(&Msg{
			ID:       msg.ID,
			Topic:    topic,
			Body:     []byte(msg.Values["body"].(string)),
			Group:    group,
			Consumer: consumer,
		})
		if err == nil {
			err := q.client.XAck(ctx, topic, group, msg.ID).Err()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
