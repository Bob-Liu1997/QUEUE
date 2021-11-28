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
	name    string
	quickCh chan Message
	slowCh  chan Message
	doneCh  chan struct{}
	state   int32
}

func NewTopic(name string) Topic {
	return &topic{
		name:    name,
		quickCh: make(chan Message, 64),
		slowCh:  make(chan Message, 1024),
		doneCh:  make(chan struct{}),
	}
}

func (t *topic) Publish(msg Message, timeout time.Duration) error {
	currentState := t.State()
	if currentState != StateRunning {
		logrus.Errorf("register topic is in wrong state, topic: %s, state: %s",
			t.Name(), String(t.State()))
		return ErrWrongState
	}
	switch msg.Priority() {
	case None:
		fallthrough
	case Low:
		select {
		case t.slowCh <- msg:
			return nil
		case <-time.After(timeout):
			return ErrPublishTimeOut
		}
	case High:
		select {
		case t.quickCh <- msg:
			return nil
		case <-time.After(timeout):
			return ErrPublishTimeOut
		}
	}
	return nil

}

func (t *topic) Subscribe(c Client) error {
	currentState := t.State()
	if  currentState != StateInit {
		logrus.Errorf("cannot subscribe the topic [%s] in wrong state [%s]", 
		t.name, String(t.state))
		return ErrWrongState
	}
	go func() {
		for {
			select {
			case msg := <-t.quickCh:
				c.OnMessageReceived(msg)
            default :
			select {
			case msg := <- t.quickCh:
				c.OnMessageReceived(msg)
			case msg := <- t.slowCh:
				c.OnMessageReceived(msg)
			case <-t.doneCh:
				for msg := range t.quickCh{
					c.OnMessageReceived(msg)
				}
				for msg := range t.slowCh{
					c.OnMessageReceived(msg)
				}
				atomic.CompareAndSwapInt32(&t.state, StateClosing, StateClosed)
				return
			}
		 }
       }
	}()
	t.setState(StateRunning)
	return nil
}

func (t *topic) Name() string {
	return t.name
}

func (t *topic) Close() error {
	if t.State() != StateRunning {
		logrus.Errorf("close topic in error state, topic: %s, state: %s", t.Name(), t.State())
		return ErrWrongState
	}  
	t.setState(StateClosing)
	logrus.Infof("topic [%s] closing", t.Name())
	close(t.doneCh)
	for t.State() == StateClosed {
		break
	}
	logrus.Infof("topic [%s] closed", t.Name())
	return nil

}

func (t *topic) State() int32 {
	return atomic.LoadInt32(&t.state)
}

func (t *topic) setState(new int32) {
	atomic.StoreInt32(&t.state, new)
}

