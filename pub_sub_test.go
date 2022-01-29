package rmq

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNewPubSubMQ(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	q := NewPubSubMQ(client)
	topic := "test"
	count := 10
	var wg sync.WaitGroup
	wg.Add(count)
	go q.Consume(context.Background(), topic, 0, func(msg *Msg) error {
		fmt.Printf("consume partiton0: %+v\n", string(msg.Body))
		wg.Done()
		return nil
	})
	go q.Consume(context.Background(), topic, 1, func(msg *Msg) error {
		fmt.Printf("consume partiton1: %+v\n", string(msg.Body))
		wg.Done()
		return nil
	})
	time.Sleep(time.Second)
	for i := 0; i < count; i++ {
		q.SendMsg(context.Background(), &Msg{
			Topic:     topic,
			Body:      []byte(topic + strconv.Itoa(i)),
			Partition: i % 2,
		})
	}
	wg.Wait()
}

func TestPubSubMQConsumeMultiPartitions(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	q := NewPubSubMQ(client)
	topic := "test"
	count := 10
	var wg sync.WaitGroup
	wg.Add(count)
	go q.ConsumeMultiPartitions(context.Background(), topic, []int{0, 1}, func(msg *Msg) error {
		fmt.Printf("consume partiton: %+v\n", msg)
		wg.Done()
		return nil
	})
	time.Sleep(time.Second)
	for i := 0; i < count; i++ {
		q.SendMsg(context.Background(), &Msg{
			Topic:     topic,
			Body:      []byte(topic + strconv.Itoa(i)),
			Partition: i % 2,
		})
	}
	wg.Wait()
}
