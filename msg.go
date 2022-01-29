package rmq

// Msg 消息
type Msg struct {
	ID        string // 消息的编号
	Topic     string // 消息的主题
	Body      []byte // 消息的Body
	Partition int    // 分区号
	Group     string // 消费者组
	Consumer  string // 消费者组里的消费者
}
