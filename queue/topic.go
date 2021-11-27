package queue

import (
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
) 

type Topic interface {
	// client publish msg by topic
	Publish(msg Message, timeout time.Duration) error
    // client subscribe message
	Subscribe(Client) error
    // topic's name
	Name() string
    // close topic by change topic's state
	Close() error
	// get topic's state
	State() int32
}

type topic struct {
	name string
	quickCh chan Message
	slowCh chan Message
	doneCh chan struct{}
	state int32
}

func NewTopic(name string) Topic {
	return &topic{
		name: name,
		quickCh: make(chan Message, 64),
		slowCh: make(chan Message, 1024),
		doneCh: make(chan struct{}),
	}
}

func (t *topic) Publish(msg Message, timeout time.Duration) error {
    currentState := t.State()
	if (currentState != StateRunning) {
        logrus.Errorf("register topic is in wrong state, topic: %s, state: %s", 
		      t.Name(), String(t.State())) 
	    return ErrWrongState
	}
	switch msg.Priority() {
	case None:
		fallthrough
	case Low:
		select {
		case  t.slowCh <- msg:
			return nil
		case <- time.After(timeout):
			return ErrPublishTimeOut
		}
	case High:
		select{
		case  t.quickCh <- msg:
			return nil
		case <- time.After(timeout):
			return ErrPublishTimeOut
		}
	}
	return nil

}

func (t *topic) Subscribe(Client) error {
    return nil
}

func (t *topic) Name() string {
    return t.name
}

func (t *topic) Close() error {
    return nil
} 

func (t *topic) State() int32 {
    return atomic.LoadInt32(&t.state)
}

func (t *topic) setState(new int32) {
	atomic.StoreInt32(&t.state, new)
}