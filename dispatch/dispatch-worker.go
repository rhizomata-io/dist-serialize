package dispatch

import (
	"fmt"
	"log"

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

	worker.dataWatcher = worker.helper.WatchData(TopicIn, worker.handleData)
	log.Println("[INFO-DSWorker] Start watching data. ", worker.ID())
	worker.helper.GetDataList(TopicIn, worker.handleData)

	return nil
}

//Stop ..
func (worker *DSWorker) Stop() error {
	worker.dataWatcher.Stop()
	worker.started = false
	log.Printf("DSWorker [%s] Stopped.\n", worker.ID())
	return nil
}

//IsStarted ..
func (worker *DSWorker) IsStarted() bool {
	return worker.started
}

//IsStarted ..
func (worker *DSWorker) handleData(key string, data []byte) {
	fmt.Println("## Handle Data :", key, string(data))
	worker.helper.DeleteData(TopicIn, key)
	// worker.helper.pu
}
