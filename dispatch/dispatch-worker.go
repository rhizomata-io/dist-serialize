package dispatch

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
)

// DSWorker implements worker.Worker
type DSWorker struct {
	id          string
	helper      *worker.Helper
	jobInfo     *JobInfo
	dataWatcher *kv.Watcher
	started     bool
}

// JobInfo job info object
type JobInfo struct {
	// InTopic  string `json:"in"`
	// OutTopic string `json:"out"`
	InitData string `json:"init"`
}

// CheckPoint CheckPoint
type CheckPoint struct {
	Count int64 `json:"count"`
}

//ID ..
func (worker *DSWorker) ID() string {
	return worker.id
}

//Start ..
func (worker *DSWorker) Start() error {
	worker.started = true
	log.Printf("DSWorker [%s] Started.\n", worker.ID())

	worker.helper.GetDataList(TopicIn, func(fullPath, rowID string, value []byte) bool {
		if worker.started {
			worker.handleData(fullPath, rowID, value)
		}
		return worker.started
	})

	worker.dataWatcher = worker.helper.WatchDataWithTopic(TopicIn,
		func(eventType kv.EventType, fullPath string, rowID string, value []byte) {
			if eventType == kv.PUT {
				worker.handleData(fullPath, rowID, value)
			}
		})
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

//IsStarted ..
func (worker *DSWorker) handleData(fullPath string, rowID string, data []byte) {
	fmt.Println("## Handle Data :", fullPath, string(data))

	worker.helper.DeleteDataFullPath(fullPath)

	random := uint32(rand.Int31n(1000))
	outData := "RTN-" + string(data) + "-" + worker.id + "-" + worker.helper.KernelID() + ":" + fmt.Sprint(random)

	time.Sleep(time.Duration(random) * time.Millisecond)

	worker.helper.PutData(TopicOut, rowID, outData)
}
