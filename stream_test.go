package rmq

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"sync"
	"testing"
)

func TestNewStreamMQ(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	q := NewStreamMQ(client, 100, true)
	topic := "test"
	count := 10
	var wg sync.WaitGroup
	wg.Add(count * 4)
	go q.Consume(context.Background(), topic, "group1", "consumer1", "$", 5, func(msg *Msg) error {
		fmt.Printf("consume group1 consumer1: %+v\n", msg)
		wg.Done()
		return nil
	})
	go q.Consume(context.Background(), topic, "group1", "consumer2", "$", 5, func(msg *Msg) error {
		fmt.Printf("consume group1 consumer2: %+v\n", msg)
		wg.Done()
		return nil
	})
	go q.Consume(context.Background(), topic, "group2", "consumer1", "$", 5, func(msg *Msg) error {
		fmt.Printf("consume group2 consumer1: %+v\n", msg)
		wg.Done()
		return nil
	})
	go q.Consume(context.Background(), topic, "group2", "consumer2", "$", 5, func(msg *Msg) error {
		fmt.Printf("consume group2 consumer2: %+v\n", msg)
		wg.Done()
		return nil
	})
	for i := 0; i < count; i++ {
		q.SendMsg(context.Background(), &Msg{
			Topic: topic,
			Body:  []byte(topic + strconv.Itoa(i)),
		})
	}
	wg.Wait()
}
