package ds

import (
	"github.com/rhizomata-io/dist-daemonize/dd"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-serialize/dispatch"
)

// Serializer ..
type Serializer struct {
}

// Serialize ..
func Serialize() (daemonizer dd.Daemonizer, err error) {
	runOptions := config.ParseRunOptions()

	daemonizer, err = dd.Daemonize(runOptions)
	if err == nil {
		factory := &dispatch.Factory{}
		daemonizer.RegisterWorkerFactory(factory)
		daemonizer.Start()
		// daemonizer.StartDiscovery()
		// daemonizer.Wait()
	}

	return daemonizer, err
}
