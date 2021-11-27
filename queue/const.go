package queue

import "errors"


// state represents the state of Queue, Client, Topic
const (
	StateInit int32 = iota
	StateRunning 
	StateClosing
	StateClosed
)

func String(state int32) string {
	switch state {
	case StateInit:
		return "Init"
	case StateRunning:
		return "Running"
	case StateClosing:
		return "Closing"
	case StateClosed:
		return "Closed"
	}
	return "Unknown"
}

type OperateType uint32

func (o OperateType) String() string {
	return ""
}

type Priority uint8

const (
	None Priority = iota
	Low 
	High

)

func (p Priority) String() string {
	switch p {
	case None:
		return "None"
	case Low:
		return "Low"
	case High:
		return "High"
	}
	return "UnKnown"
}

var ErrReplyNoWay = errors.New("no way to reply")

var ErrWaitTimeOut = errors.New("wait time out")

var ErrWrongState = errors.New("wrong state")

var ErrPublishTimeOut = errors.New("publish time out")