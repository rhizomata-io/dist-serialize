package main

import (
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-serialize/dispatch"
	"github.com/rhizomata-io/dist-serialize/ds"
	"log"
)

func main() {

	daemonizer, err := ds.Serialize()
	if err != nil {
		log.Fatal("ERROR", err)
	}
	job1 := job.NewWithPIAndID("job1", "dispatch", `{"init":"hello"}`)
	daemonizer.AddJobIfNotExists(job1)

	disp := dispatch.New(daemonizer.GetKernel())
	go disp.Put("job1", "Hellooooooooooooo")
	go disp.Put("job1", "Helluuuuuuuuuuuuu")
	daemonizer.Wait()
	// sigs := make(chan os.Signal, 1)
	// signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// <-sigs
}
