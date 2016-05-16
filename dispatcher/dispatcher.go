package dispatcher

import (
	"errors"
	"github.com/puper/go-queue/blockqueue"
	"github.com/puper/go-queue/listqueue"
	"log"
	"sync"
	"time"
)

var wg sync.WaitGroup

type (
	Dispatcher struct {
		cfg              *Config
		blockJobQueue    map[string]*blockqueue.BlockQueue
		nonblockJobQueue *blockqueue.BlockQueue
		commandQueue     *blockqueue.BlockQueue
		commandChan      chan *Command
		storage          *Storage
		server           *Server
		running          bool
		pause            bool
		mutex            sync.Mutex
		configFile       string
	}

	Command struct {
		Type string
		Data interface{}
	}
)

func (this *Dispatcher) SetConfigFile(filename string) {
	this.configFile = filename
}

func NewDispatcher(cfg *Config) (*Dispatcher, error) {
	var err error
	d := &Dispatcher{
		cfg:              cfg,
		blockJobQueue:    make(map[string]*blockqueue.BlockQueue),
		nonblockJobQueue: NewQueue(),
		commandQueue:     NewQueue(),
		commandChan:      make(chan *Command),
	}
	d.server = NewServer(cfg.Host, cfg.Port, d)
	d.storage, err = NewStorage(cfg.DataPath, cfg.Sync)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (this *Dispatcher) Init() {
	go func() {
		for {
			command, err := this.commandQueue.Get(true, 1)
			if err == nil {
				this.commandChan <- command.(*Command)
			}
		}
	}()
	this.storage.Start()
	go this.startNonblockDispatcher()
	go this.server.Start()
}

func (this *Dispatcher) Close() {
	this.server.Close()
	wg.Wait()
	this.storage.Close()
}

func (this *Dispatcher) Start() {
	this.Init()
	jobChan := this.storage.GetJobChan()
	for {
		select {
		case command := <-this.commandChan:
			if command.Type == "empty" {
				if this.blockJobQueue[command.Data.(string)].IsEmpty() {
					delete(this.blockJobQueue, command.Data.(string))
				} else {
					go this.startBlockDispatcher(command.Data.(string))
				}
			}
		case job := <-jobChan:
			for this.pause {
				time.Sleep(time.Second)
			}
			if _, ok := this.cfg.Rules[job.Type]; ok {
				if job.Key == "" {
					this.nonblockJobQueue.Put(job, false, 0)
				} else {
					if _, ok := this.blockJobQueue[job.Key]; ok {
						this.blockJobQueue[job.Key].Put(job, false, 0)
					} else {
						this.blockJobQueue[job.Key] = NewQueue()
						this.blockJobQueue[job.Key].Put(job, false, 0)
						go this.startBlockDispatcher(job.Key)
					}
				}
			} else {
				this.storage.Delete(job.Id)
			}
		}
	}
}

func (this *Dispatcher) startNonblockDispatcher() {
	for {
		job, err := this.nonblockJobQueue.Get(true, 1)
		if err == nil {
			newJob := job.(*Job)
			if _, ok := this.cfg.Rules[newJob.Type]; ok {
				newJob.Storage = this.storage
				newJob.Rule = this.cfg.Rules[newJob.Type]
				go newJob.Execute()
			} else {
				go this.storage.Delete(newJob.Id)
			}
		}
	}
}

func (this *Dispatcher) startBlockDispatcher(key string) {
	for {
		job, err := this.blockJobQueue[key].Get(true, 1)
		if _, ok := err.(*blockqueue.EmptyQueueError); ok {
			this.commandQueue.Put(&Command{
				Type: "empty",
				Data: key,
			}, false, 0)
			break
		} else if err == nil {
			newJob := job.(*Job)
			if _, ok := this.cfg.Rules[newJob.Type]; ok {
				newJob.Storage = this.storage
				newJob.Rule = this.cfg.Rules[newJob.Type]
				newJob.Execute()
			} else {
				go this.storage.Delete(newJob.Id)
			}
		}
	}
}

func (this *Dispatcher) Put(job *Job) (*Job, error) {
	if _, ok := this.cfg.Rules[job.Type]; ok {
		return this.storage.Put(job)
	}
	return nil, errors.New("no handler for this job")
}

func (this *Dispatcher) Pause() {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.pause = true
	wg.Wait()
}

func (this *Dispatcher) Continue() {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if this.pause {
		this.pause = false
	}
}

func (this *Dispatcher) Reload() error {
	this.Pause()
	cfg, err := NewConfigWithFile(this.configFile)
	if err != nil {
		log.Println(err)
		return err
	}
	this.cfg = cfg
	this.Continue()
	return nil
}

func NewQueue() *blockqueue.BlockQueue {
	return blockqueue.NewBlockQueue(listqueue.NewListQueue(), 0)
}
