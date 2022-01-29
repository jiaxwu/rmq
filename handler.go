package rmq

// Handler 返回值代表消息是否消费成功
type Handler func(msg *Msg) error
