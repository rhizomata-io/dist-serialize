package dispatch

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rhizomata-io/dist-daemonize/kernel"
	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
)

const (
	// TopicIn input topic
	TopicIn = "disp-in"
	// TopicOut output topic
	TopicOut = "disp-out"
)

//Dispatch ...
type Dispatch struct {
	kernel      *kernel.Kernel
	helperMap   map[string]*worker.Helper
	counterLock sync.Mutex
	counter     uint64
	// discovery discovery.Discovery
}

// New ...
func New(kernel *kernel.Kernel) (dispatch *Dispatch) {
	dispatch = &Dispatch{kernel: kernel, helperMap: make(map[string]*worker.Helper)}
	dispatch.counter = uint64(time.Now().Unix())
	return dispatch
}

func (dispatch *Dispatch) newRowID() (rowID string) {
	dispatch.counterLock.Lock()
	dispatch.counter = dispatch.counter + 1
	counter := dispatch.counter
	dispatch.counterLock.Unlock()

	rowID = fmt.Sprintf("%s-%X", dispatch.kernel.ID(), counter)
	return rowID
}

func (dispatch *Dispatch) getHelper(jobid string) (helper *worker.Helper, err error) {
	helper = dispatch.helperMap[jobid]

	if helper == nil {
		job, err := dispatch.kernel.GetJobManager().GetJob(jobid)
		if err != nil {
			log.Println("[ERROR] Get Job for ", jobid, err)
			return nil, err
		}

		helper = dispatch.kernel.GetWorkerManager().NewHelper(&job)
		dispatch.helperMap[jobid] = helper
	}

	return helper, err
}

// Put ...
func (dispatch *Dispatch) Put(jobid string, data interface{}) (resp []byte, err error) {
	helper, err := dispatch.getHelper(jobid)

	if err != nil {
		return nil, err
	}

	rowID := dispatch.newRowID()
	err = helper.PutObject(TopicIn, rowID, data)
	if err != nil {
		return nil, err
	}

	var wait chan string

	watcher := helper.WatchData(TopicOut,
		func(key string, value []byte) {
			fmt.Println("%%% Watch ", key)
			resp = value
			wait <- "sss"
		})

	defer watcher.Stop()
	<-wait

	return resp, err
}
