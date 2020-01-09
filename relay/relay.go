package relay

import (
	"fmt"
	"github.com/rhizomata-io/dist-daemonize/kernel"
	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
	"log"
	"sync"
	"time"
)

//Relay ...
type Relay struct {
	kernel      *kernel.Kernel
	helperMap   map[string]*worker.Helper
	counterLock sync.Mutex
	counter     uint64
	// discovery discovery.Discovery
}

// New ...
func New(kernel *kernel.Kernel) (relay *Relay) {
	relay = &Relay{kernel: kernel, helperMap: make(map[string]*worker.Helper)}
	relay.counter = uint64(time.Now().Unix())
	return relay
}

func (relay *Relay) newRowID() (rowID string) {
	relay.counterLock.Lock()
	relay.counter = relay.counter + 1
	counter := relay.counter
	relay.counterLock.Unlock()

	rowID = fmt.Sprintf("%s-%X", relay.kernel.ID(), counter)
	return rowID
}

func (relay *Relay) getHelper(jobid string) (helper *worker.Helper, err error) {
	helper = relay.helperMap[jobid]

	if helper == nil {
		job, err := relay.kernel.GetJobManager().GetJob(jobid)
		if err != nil {
			log.Println("[ERROR] Get Job for ", jobid, err)
			return nil, err
		}

		helper = relay.kernel.GetWorkerManager().NewHelper(&job)
		relay.helperMap[jobid] = helper
	}

	return helper, err
}

// Put ...
func (relay *Relay) Put(jobid string, data interface{}) (resp []byte, err error) {
	helper, err := relay.getHelper(jobid)

	if err != nil {
		return nil, err
	}

	err = helper.PutObject("in", relay.newRowID(), data)
	return resp, err
}
