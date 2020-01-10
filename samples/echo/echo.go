package main

import (
	"log"
	"strconv"

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

	job1 := job.NewWithPIAndID("job1", "dispatch", `{"init":"hello"}`)
	daemonizer.AddJobIfNotExists(job1)

	disp := dispatch.New(daemonizer.GetKernel())

	for i := 0; i < 2; i++ {
		go disp.Put("job1", "J-"+strconv.Itoa(i))
		// go disp.Put("job1", "Hellooooooooooooo-"+strconv.Itoa(i))
		// go disp.Put("job1", "Helluuuuuuuuuuuuu-"+strconv.Itoa(i))
		// go disp.Put("job1", "Hiiiiiiiiiiiiiiii-"+strconv.Itoa(i))
	}
	daemonizer.Wait()
	// sigs := make(chan os.Signal, 1)
	// signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// <-sigs
}
