package dispatch

import (
	"github.com/rhizomata-io/dist-daemonize/kernel/worker"
	"log"
)

// Factory implements worker.Factory
type Factory struct {
	name     string
	handlers map[string]Handler
}

// NewFactory create new Factory
func NewFactory(name string) (factory *Factory) {
	factory = &Factory{name: name, handlers: make(map[string]Handler)}
	return factory
}

// Name implements worker.Factory.Name as 'dispatch'
func (factory *Factory) Name() string { return factory.name }

// RegisterHandler ..
func (factory *Factory) RegisterHandler(name string, handler Handler) {
	factory.handlers[name] = handler
}

// NewWorker implements worker.Factory.NewWorker
func (factory *Factory) NewWorker(helper *worker.Helper) (worker worker.Worker, err error) {
	jobInfo := &JobInfo{}
	helper.Job().GetAsObject(jobInfo)

	// log.Println("helper.ID()::", helper.ID())
	// log.Println("JOB::", helper.Job())
	// log.Println("jobInfo::", jobInfo)
	// log.Println("job Data::", string(helper.Job().Data))
	handler := factory.handlers[jobInfo.Handler]
	if handler == nil {
		log.Fatal("[FATAL-Dispatch.Factory] No handler for ", jobInfo.Handler)
	}
	worker = &DSWorker{id: helper.ID(), helper: helper, jobInfo: jobInfo, handler: handler}
	return worker, err
}
