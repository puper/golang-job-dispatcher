package blockqueue

import (
	"sync"
	"time"
)

type EmptyQueueError struct{}

type FullQueueError struct{}

func (err *EmptyQueueError) Error() string {
	return "Queue is Empty"
}

func (err *FullQueueError) Error() string {
	return "Queue is Full"
}

type QueueIF interface {
	Put(v interface{}) error
	Get() (interface{}, error)
	Size() int
}

type BlockQueue struct {
	queue    QueueIF
	mutex    *sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	maxSize  int
}

func NewBlockQueue(queue QueueIF, maxSize int) *BlockQueue {
	q := new(BlockQueue)
	q.queue = queue
	q.maxSize = maxSize
	q.mutex = &sync.Mutex{}
	q.notEmpty = sync.NewCond(q.mutex)
	q.notFull = sync.NewCond(q.mutex)
	return q
}

func (q *BlockQueue) Size() int {
	q.mutex.Lock()
	size := q.queue.Size()
	q.mutex.Unlock()
	return size
}

func (q *BlockQueue) IsEmpty() bool {
	q.mutex.Lock()
	isEmpty := (q.queue.Size() == 0)
	q.mutex.Unlock()
	return isEmpty
}

func (q *BlockQueue) IsFull() bool {
	q.mutex.Lock()
	isFull := (q.maxSize > 0 && q.queue.Size() == q.maxSize)
	q.mutex.Unlock()
	return isFull
}

func (q *BlockQueue) Get(block bool, timeout uint) (interface{}, error) {
	q.notEmpty.L.Lock()
	defer q.notEmpty.L.Unlock()
	empty := false
	if !block {
		if q.queue.Size() == 0 {
			empty = true
		}
	} else if timeout == 0 {
		for q.queue.Size() == 0 {
			q.notEmpty.Wait()
		}
	} else {
		if q.queue.Size() == 0 {
			timer := time.After(time.Duration(timeout) * time.Second)
			notEmpty := make(chan bool, 1)
			go q.waitSignal(q.notEmpty, notEmpty)
		TIMEOUT:
			for q.queue.Size() == 0 {
				select {
				case <-timer:
					empty = true
					q.cancelWait(q.notEmpty, notEmpty)
					break TIMEOUT
				case <-notEmpty:
					if q.queue.Size() == 0 {
						go q.waitSignal(q.notEmpty, notEmpty)
					} else {
						break TIMEOUT
					}
				}
			}
			close(notEmpty)
		}
	}
	if empty {
		return nil, &EmptyQueueError{}
	}
	v, err := q.queue.Get()
	if err != nil {
		return nil, err
	}
	q.notFull.Signal()
	return v, nil
}

func (q *BlockQueue) Put(v interface{}, block bool, timeout uint) error {
	q.notFull.L.Lock()
	defer q.notFull.L.Unlock()
	full := false
	if q.maxSize > 0 {
		if !block {
			if q.queue.Size() == q.maxSize {
				full = true
			}
		} else if timeout == 0 {
			for q.queue.Size() == q.maxSize {
				q.notFull.Wait()
			}
		} else {
			if q.queue.Size() == q.maxSize {
				timer := time.After(time.Duration(timeout) * time.Second)
				notFull := make(chan bool, 1)
				go q.waitSignal(q.notFull, notFull)
			TIMEOUT:
				for q.queue.Size() == q.maxSize {
					select {
					case <-timer:
						full = true
						q.cancelWait(q.notFull, notFull)
						break TIMEOUT
					case <-notFull:
						if q.queue.Size() == q.maxSize {
							go q.waitSignal(q.notFull, notFull)
						} else {
							break TIMEOUT
						}
					}
				}
				close(notFull)
			}
		}
	}
	if full {
		return &FullQueueError{}
	}
	err := q.queue.Put(v)
	if err != nil {
		return err
	}
	q.notEmpty.Signal()
	return nil
}

func (q *BlockQueue) waitSignal(cond *sync.Cond, c chan<- bool) {
	cond.Wait()
	c <- true
}

func (q *BlockQueue) cancelWait(cond *sync.Cond, c <-chan bool) {
	cond.Signal()
	<-c
}
