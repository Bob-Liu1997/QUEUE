package queue

import (
	"sync/atomic"
	"time"
	"github.com/sirupsen/logrus"
)

type Client interface {
	// Subscribe will register a topic, and put it in queue
	SubScribe(topic string) error
	// chan to receive message
	ReceiveCh() <- chan Message 
	// publish message and wait reply
	PublishAndWait(
		msg Message,
		publishTimeout, 
		waitTimeout time.Duration) (reply Message, err error)
	// publish message
	Publish(msg Message, timeout time.Duration) error
	// wait time to be relay
	Wait(msg Message, timeout time.Duration) (reply Message, err error)
	// receive message from topic in queue
	OnMessageReceived(Message)
	// close client, will wait the bound topic close first
	Close()
	// get bound topic name
	TopicName() string
    // 
	State() int32
}

type client struct {
	q Queue
    t Topic
	receiveCh chan Message
	state int32
}

func NewClient(q Queue) Client {
	return &client{
		q: q,
		receiveCh: make(chan Message),
	}

}

func (c *client) SubScribe(topic string) (err error) {
	if c.State() != StateInit {
		logrus.Errorf("cannot subscribe the topic [%s] in wrong state [%s]", 
		topic, String(c.state))
		err =  ErrWrongState
		return
	}
	var t Topic
	if t, err = c.q.RegisterTopic(topic); err != nil {
        logrus.Errorf("cann't register topic [%s]", topic)
		return err
	} 
	c.t = t
	if err = t.Subscribe(c); err != nil {
		logrus.Errorf("cannot subscribe topic [%s], err: %v", topic, err)
		return
	}
	logrus.Debug("topic [%s] register", topic)
	c.setState(StateRunning)
	return

}

func (c *client) ReceiveCh() <-chan Message {
	return c.receiveCh

}

func (c *client) PublishAndWait(
		msg Message,
		publishTimeout, 
		waitTimeout time.Duration) (reply Message, err error) {
	err = c.Publish(msg, publishTimeout)
    if err != nil {
		return nil, err
	}
	return c.Wait(msg, waitTimeout)

}


func (c *client) Publish(msg Message, timeout time.Duration) error {
	t := c.t
	if t.State() != StateRunning {
		logrus.Errorf("Publish msg for [%s] topic in wrong [%s] state",
		t.Name(), String(t.State()))
		return ErrWrongState
	}
	err := t.Publish(msg, timeout)
	return err
}

func (c *client) Wait(msg Message, timeout time.Duration) (reply Message, err error){
    reply, err = msg.Wait(timeout)
	return
}

func (c *client) OnMessageReceived(msg Message) {
    c.receiveCh <- msg
}

func (c *client) Close() {
	if c.state != StateRunning {
		logrus.Errorf("Close client in wrong [%s] state",String(c.State()))
		return
	}
	c.setState(StateClosing)
	c.t.Close()

	c.setState(StateClosed)
	logrus.Infof("client closed, topic: %s", c.TopicName())


}

func (c *client) TopicName() string {
    return c.t.Name()
}

func (c *client) State() int32 {
	return atomic.LoadInt32(&c.state)
}

func (c *client) setState(s int32) {
	atomic.StoreInt32(&c.state, s)
}