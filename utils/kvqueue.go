package utils

import (
	"container/list"
	"sync"
)

// KVQueue ...
type KVQueue struct {
	sync.Mutex
	innerList *list.List
	kvMap     map[interface{}]interface{}
	lock      chan bool
	waiting   int64
}

//NewKVQueue ...
func NewKVQueue() *KVQueue {
	queue := KVQueue{innerList: list.New(), kvMap: make(map[interface{}]interface{}), lock: make(chan bool)}
	return &queue
}

// Push ..
func (queue *KVQueue) Push(key interface{}, value interface{}) {
	queue.Lock()
	oldval := queue.kvMap[key]
	if oldval == nil {
		queue.innerList.PushBack(key)
	} // else {
	// 	fmt.Println("----------------- over write : ", key, oldval, value)
	// }
	queue.kvMap[key] = value

	if queue.waiting > 0 {
		queue.lock <- false
	}
	queue.Unlock()
}

// Pop ..
func (queue *KVQueue) Pop() (key interface{}, value interface{}) {
	key = queue._pop()

	for ; key == nil; key = queue._pop() {
		queue.waiting = queue.waiting + 1
		<-queue.lock
		queue.waiting = queue.waiting - 1
	}

	value = queue.kvMap[key]
	return key, value
}

// Pop ..
func (queue *KVQueue) _pop() (key interface{}) {
	queue.Lock()
	el := queue.innerList.Front()
	if el != nil {
		key = el.Value
		queue.innerList.Remove(el)
	}
	queue.Unlock()
	return key
}
