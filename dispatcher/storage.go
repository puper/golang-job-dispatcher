package dispatcher

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"sync"
)

type Storage struct {
	id        uint64
	backend   *leveldb.DB
	jobChan   chan *Job
	putSignal chan struct{}
	mutex     sync.Mutex
	sync      bool
	wg        sync.WaitGroup
	running   bool
}

func NewStorage(filename string, sync bool) (s *Storage, err error) {
	s = new(Storage)
	s.sync = sync
	s.backend, err = leveldb.OpenFile(filename, nil)
	if err != nil {
		return nil, err
	}
	iter := s.backend.NewIterator(nil, nil)
	if iter.Last() {
		s.id = byteToUint64(iter.Key())
	}
	iter.Release()
	s.jobChan = make(chan *Job, 512)
	s.putSignal = make(chan struct{}, 1)
	return
}

func (this *Storage) Start() {
	this.wg.Add(1)
	this.running = true
	go this.start()
}

func (this *Storage) start() {
	defer this.wg.Done()
	var key uint64 = 0
	for this.running {
		iter := this.backend.NewIterator(&util.Range{Start: uint64ToByte(key + 1)}, nil)
		for iter.Next() && this.running {
			key = byteToUint64(iter.Key())
			this.jobChan <- NewJob(key, iter.Value())
		}
		iter.Release()
		<-this.putSignal
	}
}

func (this *Storage) Close() error {
	this.running = false
	this.wg.Wait()
	return this.backend.Close()
}

func (this *Storage) GetJobChan() chan *Job {
	return this.jobChan
}

func (this *Storage) Put(job *Job) (*Job, error) {
	this.mutex.Lock()
	this.id = this.id + 1
	id := this.id
	job.Id = this.id
	this.mutex.Unlock()
	err := this.backend.Put(uint64ToByte(id), job.Bytes(), &opt.WriteOptions{Sync: this.sync})
	select {
	case this.putSignal <- struct{}{}:
	default:
	}
	return job, err
}

func (this *Storage) Delete(id uint64) (err error) {
	err = this.backend.Delete(uint64ToByte(id), nil)
	return
}
