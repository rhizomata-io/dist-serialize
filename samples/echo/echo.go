package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/rhizomata-io/dist-daemonize/dd"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-serialize/dispatch"
)

func main() {
	runOptions := config.ParseRunOptions()

	daemonizer, err := dd.Daemonize(runOptions)
	if err == nil {
		factory := dispatch.NewFactory("dispatch")

		handler := func(command *dispatch.Command) string {
			fmt.Println("## Handle Data :", command.FullPath, string(command.Data), command.CommandCnt)
			random := uint32(rand.Int31n(100))
			outData := fmt.Sprintf("RTN-%s-%s-%s:%d [%d]", command.Data, command.JobInfo.Config["target"], runOptions.Name, random, command.CommandCnt)
			time.Sleep(time.Duration(random) * time.Millisecond)
			return outData
		}

		factory.RegisterHandler("echo", handler)

		daemonizer.RegisterWorkerFactory(factory)

		daemonizer.Start()
		// daemonizer.StartDiscovery()
		// daemonizer.Wait()
	} else {
		log.Fatal("ERROR", err)
	}

	job1 := job.NewWithPIAndID("job1", "dispatch", `{"handler":"echo", "config":{"target":"A"}}`)
	daemonizer.AddJobIfNotExists(job1)

	job2 := job.NewWithPIAndID("job2", "dispatch", `{"handler":"echo", "config":{"target":"B"}}`)
	daemonizer.AddJobIfNotExists(job2)

	job3 := job.NewWithPIAndID("job3", "dispatch", `{"handler":"echo", "config":{"target":"C"}}`)
	daemonizer.AddJobIfNotExists(job3)

	daemonizer.Wait()
}
