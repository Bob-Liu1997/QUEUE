package queue

import "testing"

func TestQueueType(t *testing.T) {
	var m Message
	m = &msg {
		topic: "DB",
	}
	t.Log(m.Topic())
}