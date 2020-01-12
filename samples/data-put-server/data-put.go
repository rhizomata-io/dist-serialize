package main

import (
	"log"

	"github.com/rhizomata-io/dist-daemonize/dd"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-serialize/dispatch"
)

func main() {
	runOptions := config.ParseRunOptions()

	daemonizer, err := dd.Daemonize(runOptions)
	if err != nil {
		log.Fatal("ERROR", err)
	}

	disp := dispatch.New(daemonizer.GetKernel())

	dispatch.SupportAPI(disp, daemonizer.GetAPIServer())

	daemonizer.Start()

	daemonizer.Wait()
}
