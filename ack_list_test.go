package rmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"sync"
	"testing"
)

func TestNewACKListMQ(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	q := NewACKListMQ(client)
	topic := "test"
	count := 10
	var wg sync.WaitGroup
	wg.Add(count)
	i := 0
	go q.Consume(context.Background(), topic, func(msg *Msg) error {
		i++
		if i%2 == 0 {
			fmt.Printf("consume partiton0: %+v\n", msg)
			wg.Done()
			return nil
		} else {
			fmt.Printf("消费失败: %+v\n", msg)
			return errors.New("消费失败")
		}
	})
	for i := 0; i < count; i++ {
		q.SendMsg(context.Background(), &Msg{
			Topic: topic,
			Body:  []byte(topic + strconv.Itoa(i)),
		})
	}
	wg.Wait()
}
