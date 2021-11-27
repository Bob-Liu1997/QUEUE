package queue

import (
	"fmt"
	"time"
)

// Message is used to communicate by client
type Message interface {
	// get message's topic(every message belongs to certain topic)
	Topic() string

	// a topic has some message, they are distinguished by the field
	Action() OperateType

	Priority() Priority

	// the field is loaded by message
	Payload() interface{}

	Error() error

	String() string

	// subscriber reply the message
	Reply(Message) error

	Wait(timeout time.Duration) (reply Message, err error)

	Recycle()
}

type msg struct {
	topic    string
	action   OperateType
	priority Priority
	payload  interface{}
	replyCh  chan Message 
	callback func(Message)
}

func (m *msg) Topic() string {
	return m.topic
}

func (m *msg) Action() OperateType {
	return m.action
}

func (m *msg) Priority() Priority {
	return m.priority
}

func (m *msg) Payload() interface{} {
	return m.payload
}

func (m *msg) Reply(reply Message) error {
	if m.replyCh == nil && m.callback == nil {
		return ErrReplyNoWay
	}
	if m.replyCh != nil {
        m.replyCh <- reply
	}
	if m.callback != nil {
		go m.callback(reply)
	}
	return nil
}

func (m *msg) Wait(timeout time.Duration) (reply Message, err error) {
	select {
	case message := <- m.replyCh :
		return message, nil
	case <- time.After(timeout):
		return nil, ErrWaitTimeOut
	}
}

func (m *msg) Error() error {
	if err, ok := m.Payload().(error); ok {
		return err
	}
	return nil
}

func (m *msg) Recycle() {
	m.topic = ""
	m.action = 0
	m.replyCh = nil
	m.callback = nil
	m.payload = nil
	m.priority = None
}

func (m *msg) String() string{
	return fmt.Sprintf("message{topic:%s, action:%s, payload:%v, priority: %s}",
            m.Topic(),m.action.String(),m.Payload(),m.Priority().String())
}