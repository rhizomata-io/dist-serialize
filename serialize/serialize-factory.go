package serialize

import (
	"fmt"
	"log"

	"github.com/rhizomata-io/dist-daemonize/kernel/kv"
	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
)

// Factory implements worker.Factory
type Factory struct {
}

// Name implements worker.Factory.Name as 'ds'
func (factory *Factory) Name() string { return "ds" }

// NewWorker implements worker.Factory.NewWorker
func (factory *Factory) NewWorker(helper *worker.Helper) (worker worker.Worker, err error) {
	jobInfo := &JobInfo{}
	helper.Job().GetAsObject(jobInfo)

	log.Println("helper.ID()::", helper.ID())
	log.Println("JOB::", helper.Job())
	log.Println("jobInfo::", jobInfo)
	log.Println("job Data::", string(helper.Job().Data))

	worker = &DSWorker{id: helper.ID(), helper: helper, jobInfo: jobInfo}
	return worker, err
}

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
	InTopic   string `json:"in"`
	OutTopic  string `json:"out"`
	TargetURL string `json:"target"`
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
	log.Printf("Sample Worker [%s] Started.\n", worker.ID())

	worker.dataWatcher = worker.helper.WatchData(worker.jobInfo.InTopic, worker.handleData)
	log.Println("[INFO-DSWorker] Start watching data. ", worker.ID())
	worker.helper.GetDataList(worker.jobInfo.InTopic, worker.handleData)

	return nil
}

//Stop ..
func (worker *DSWorker) Stop() error {
	worker.dataWatcher.Stop()
	worker.started = false
	log.Printf("Sample Worker [%s] Stopped.\n", worker.ID())
	return nil
}

//IsStarted ..
func (worker *DSWorker) IsStarted() bool {
	return worker.started
}

//IsStarted ..
func (worker *DSWorker) handleData(key string, data []byte) {
	fmt.Println("## Handle Data :", string(data))
	worker.helper.DeleteData(worker.jobInfo.InTopic, key)
}
