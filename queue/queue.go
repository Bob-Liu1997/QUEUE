package queue

import (
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

type Queue interface {
	Name() string
    RegisterTopic(name string) (Topic, error)
    GetTopic(name string) (Topic, error)
	Start() error
	Close()
	State() int32
}

type queue struct {
    name string
	topics sync.Map
	state int32
}

func init() {
	logrus.SetLevel(logrus.Level(logrus.InfoLevel))
}

func NewQueue(name string) Queue{
	return &queue{
		name: name,
		topics: sync.Map{},
	}
}

func (q *queue) Start() error {
	if q.State() != StateInit {
        logrus.Errorf("cannot start queue in %s state", String(q.State()))
		return ErrWrongState
	}
    q.setState(StateRunning)
	return nil
}

func (q *queue) Name() string {
    return q.name
}

func (q *queue) RegisterTopic(name string) (Topic, error) {
    if q.State() != StateRunning {
		logrus.Errorf("register topic in wrong state, topic: %s, state: %s",
		 name, String(q.State()))
		 return nil, ErrWrongState
	}
	if _, ok := q.topics.Load(name); ok {
        logrus.Errorf("topic [%s] has existed", name)
		return nil, ErrTopicExisted(name)

	}
	t := NewTopic(name)
	q.topics.Store(name, t)
	return t, nil

}

func (q *queue)  GetTopic(name string) (Topic, error) {
    topic, ok := q.topics.Load(name)
	if !ok {
		return nil, ErrTopicNotFound(name)
	}
	return topic.(Topic), nil
}

func (q *queue) Close() {
    if q.State() != StateRunning {
		logrus.Errorf("close queue in wrong state, state: %s", String(q.State()))
	    return
	}
	q.setState(StateClosed)
	logrus.Infof("queue [%s] closed", q.Name())
}

func (q *queue) setState(state int32) {
	atomic.StoreInt32(&q.state, state)
}

func (q *queue) State() int32 {
   return atomic.LoadInt32(&q.state)
}