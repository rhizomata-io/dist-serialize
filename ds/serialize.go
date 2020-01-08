package ds

import (
	"github.com/rhizomata-io/dist-daemonize/dd"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-serialize/serialize"
)

// Serializer ..
type Serializer struct {
}

// Serialize ..
func Serialize() {
	runOptions := config.ParseRunOptions()

	if daemonizer, err := dd.Daemonize(runOptions); err == nil {
		factory := &serialize.Factory{FactoryName: "serialize"}
		daemonizer.RegisterWorkerFactory(factory)
		daemonizer.Start()
		daemonizer.StartDiscovery()
		daemonizer.Wait()
	}
}
