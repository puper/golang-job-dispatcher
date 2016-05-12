package listqueue

import (
	"container/list"
)

type ListQueue struct {
	list *list.List
}

func NewListQueue() *ListQueue {
	return &ListQueue{
		list: list.New(),
	}
}

func (this *ListQueue) Put(v interface{}) error {
	this.list.PushBack(v)
	return nil
}

func (this *ListQueue) Get() (interface{}, error) {
	e := this.list.Front()
	this.list.Remove(e)
	return e.Value, nil
}

func (this *ListQueue) Size() int {
	return this.list.Len()
}
