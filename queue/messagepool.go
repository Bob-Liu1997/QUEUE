package queue

import "sync"

type MessagePool interface {
	GetMessage(
		topic string,
		action OperateType,
		payload interface{},
		callback func(Message),
		replyCh chan Message) Message
	RecycleMessages(...Message)
}

type msgPool struct {
	pool *sync.Pool
	
}

var pool MessagePool

func init() {
	pool = newMessagePool()
}

func GetNoReplyMessage(payload interface{}) Message {
	return getMessage("", 0, payload, nil, nil)
}

func GetErrorMessage(err error) Message {
	return getMessage("", 0, err, nil, nil)
}

func GetMessage(topic string, action OperateType, payload interface{}) Message {
    return getMessage(topic, action, payload, nil ,make(chan Message, 1))
}


func newMessagePool() MessagePool {
	return &msgPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &msg{
					replyCh: make(chan Message, 1),
				}
			},
		},
	}
}

func getMessage(
	topic string,
	action OperateType,
	payload interface{},
	callback func(Message),
	replyCh chan Message) Message {
	return pool.GetMessage(topic, action, payload, callback, replyCh)
}


func (p *msgPool) GetMessage(
	topic string,
	action OperateType,
	payload interface{},
	callback func(Message),
	replyCh chan Message) Message {
	message := p.pool.Get().(*msg)
    message.topic = topic
	message.action = action
	message.payload = payload
	message.callback =callback
	message.replyCh = replyCh
	return message
}

func (p *msgPool) RecycleMessages(messages ...Message) {
    for _, message := range messages {
		if message == nil {
			continue
		}
		message.Recycle()
		p.pool.Put(message)
	}
}