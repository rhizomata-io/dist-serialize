package utils

import (
	"container/list"
	"sync"
)

// Queue ...
type Queue struct {
	sync.Mutex
	innerList *list.List
	lock      chan bool
	waiting   int64
}

//NewQueue ...
func NewQueue() *Queue {
	queue := Queue{innerList: list.New(), lock: make(chan bool)}
	return &queue
}

// Push ..
func (queue *Queue) Push(value interface{}) {
	queue.Lock()
	queue.innerList.PushBack(value)
	queue.innerList.Init()
	if queue.waiting > 1 {
		queue.lock <- false
	}
	queue.Unlock()
}

// Pop ..
func (queue *Queue) Pop() (value interface{}) {
	value = queue._pop()

	for ; value == nil; value = queue._pop() {
		queue.waiting = queue.waiting + 1
		<-queue.lock
		queue.waiting = queue.waiting - 1
	}

	return value
}

// Pop ..
func (queue *Queue) _pop() (value interface{}) {
	queue.Lock()
	el := queue.innerList.Front()
	if el != nil {
		value = el.Value
		queue.innerList.Remove(el)
	}
	queue.Unlock()
	return value
}
