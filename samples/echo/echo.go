package main

import (
	"log"

	"github.com/rhizomata-io/dist-daemonize/dd"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-serialize/dispatch"
)

func main() {
	runOptions := config.ParseRunOptions()

	daemonizer, err := dd.Daemonize(runOptions)
	if err == nil {
		factory := &dispatch.Factory{}
		daemonizer.RegisterWorkerFactory(factory)
		daemonizer.Start()
		// daemonizer.StartDiscovery()
		// daemonizer.Wait()
	} else {
		log.Fatal("ERROR", err)
	}

	job1 := job.NewWithPIAndID("job1", "dispatch", `{"init":"helloA"}`)
	daemonizer.AddJobIfNotExists(job1)

	job2 := job.NewWithPIAndID("job2", "dispatch", `{"init":"helloB"}`)
	daemonizer.AddJobIfNotExists(job2)

	job3 := job.NewWithPIAndID("job3", "dispatch", `{"init":"helloC"}`)
	daemonizer.AddJobIfNotExists(job3)

	// disp := dispatch.New(daemonizer.GetKernel())

	daemonizer.Wait()
	// sigs := make(chan os.Signal, 1)
	// signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// <-sigs
}
