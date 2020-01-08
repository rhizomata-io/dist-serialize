package serialize

import (
	"log"

	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
)

// Factory implements worker.Factory
type Factory struct {
	FactoryName string
}

// Name implements worker.Factory.Name as 'ds'
func (factory *Factory) Name() string { return factory.FactoryName }

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
