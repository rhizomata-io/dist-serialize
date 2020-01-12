package serialize

import (
	"log"
	"sync"
	"time"

	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
	"github.com/rhizomata-io/dist-serialize/utils"
)

const (
	// TopicIn input topic
	TopicIn = "disp-in"
	// TopicOut output topic
	TopicOut = "disp-out"
)

// Handler handler function
type Handler func(command *Command) string

// DSWorker implements worker.Worker
type DSWorker struct {
	id             string
	helper         *worker.Helper
	jobInfo        *JobInfo
	dataWatcher    *kv.Watcher
	handler        Handler
	started        bool
	queue          *utils.KVQueue
	commandCounter uint64
	counterLock    sync.Mutex
}

// JobInfo job info object
type JobInfo struct {
	Handler string                 `json:"handler"`
	Config  map[string]interface{} `json:"config"`
}

//Command command object
type Command struct {
	CommandCnt uint64
	FullPath   string
	RowID      string
	JobInfo    *JobInfo
	Data       []byte
}

//ID ..
func (worker *DSWorker) ID() string {
	return worker.id
}

//Start ..
func (worker *DSWorker) Start() error {
	worker.queue = utils.NewKVQueue()

	worker.started = true
	log.Printf("DSWorker [%s] Started.\n", worker.ID())

	worker.dataWatcher = worker.helper.WatchDataWithTopic(TopicIn,
		func(eventType kv.EventType, fullPath string, rowID string, value []byte) {
			if eventType == kv.PUT {
				worker.put(fullPath, rowID, value)
				// worker.processData(fullPath, rowID, value)
				// etcd watch stream problem
				time.Sleep(2 * time.Millisecond)
			}
		})

	worker.helper.GetDataList(TopicIn, func(fullPath, rowID string, value []byte) bool {
		if worker.started {
			worker.put(fullPath, rowID, value)
			// worker.processData(fullPath, rowID, value)
		}
		return worker.started
	})

	go func() {
		worker.handleData()
	}()

	log.Println("[INFO-DSWorker] Start watching data. ", worker.ID(), worker.dataWatcher)

	return nil
}

//Stop ..
func (worker *DSWorker) Stop() error {
	worker.started = false

	if worker.dataWatcher != nil {
		worker.dataWatcher.Stop()
	} else {
		log.Println("[WARN-DispatchWorker] worker.dataWatcher is nil")
	}

	log.Printf("DSWorker [%s] Stopped.\n", worker.ID())
	return nil
}

//IsStarted ..
func (worker *DSWorker) IsStarted() bool {
	return worker.started
}

func (worker *DSWorker) processData(fullPath string, rowID string, data []byte) {
	command := &Command{JobInfo: worker.jobInfo, FullPath: fullPath, RowID: rowID, Data: data}

	worker.counterLock.Lock()
	cnt := worker.commandCounter + 1
	command.CommandCnt = cnt
	worker.commandCounter = cnt
	worker.counterLock.Unlock()

	worker.helper.DeleteDataFullPath(fullPath)

	log.Printf("[INFO] Handle Command[%s] on worker %s\n", rowID, worker.ID())
	outData := worker.handler(command)

	worker.helper.PutData(TopicOut, command.RowID, outData)
}

func (worker *DSWorker) put(fullPath string, rowID string, data []byte) {
	command := &Command{JobInfo: worker.jobInfo, FullPath: fullPath, RowID: rowID, Data: data}
	worker.queue.Push(rowID, command)

	worker.counterLock.Lock()
	cnt := worker.commandCounter + 1
	command.CommandCnt = cnt
	worker.commandCounter = cnt
	worker.counterLock.Unlock()

	log.Printf("[DEBUG] Push Command to Queue[%d] for worker %s\n", worker.queue.Size(), worker.ID())
}

func (worker *DSWorker) handleData() {
	for worker.started {
		_, oCmd := worker.queue.Pop()

		command := oCmd.(*Command)

		worker.helper.DeleteDataFullPath(command.FullPath)

		log.Printf("[INFO] Handle Command[%s] on worker %s\n", command.RowID, worker.ID())
		outData := worker.handler(command)

		worker.helper.PutData(TopicOut, command.RowID, outData)
	}
}
