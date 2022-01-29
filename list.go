package rmq

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type ListMQ struct {
	client *redis.Client // Redis客户端
}

func NewListMQ(client *redis.Client) *ListMQ {
	return &ListMQ{client: client}
}

func (q *ListMQ) SendMsg(ctx context.Context, msg *Msg) error {
	return q.client.LPush(ctx, msg.Topic, msg.Body).Err()
}

// Consume 返回值代表消费过程中遇到的无法处理的错误
func (q *ListMQ) Consume(ctx context.Context, topic string, h Handler) error {
	for {
		// 获取消息
		result, err := q.client.BRPop(ctx, 0, topic).Result()
		if err != nil {
			return err
		}
		// 处理消息
		h(&Msg{
			Topic: result[0],
			Body:  []byte(result[1]),
		})
	}
}
