package main

import (
	"fmt"
	"log"
	"math/rand"

	"github.com/rhizomata-io/dist-daemonize/dd"
	"github.com/rhizomata-io/dist-daemonize/kernel/config"
	"github.com/rhizomata-io/dist-daemonize/kernel/job"
	"github.com/rhizomata-io/dist-serialize/dispatch"
	"github.com/rhizomata-io/dist-serialize/serialize"
)

func main() {
	runOptions := config.ParseRunOptions()

	daemonizer, err := dd.Daemonize(runOptions)
	if err != nil {
		log.Fatal("ERROR", err)
		return
	}

	factory := serialize.NewFactory("dispatch")

	handler := func(command *serialize.Command) string {
		fmt.Println("## Handle Data :", command.FullPath, string(command.Data), command.CommandCnt)
		// random := uint32(rand.Int31n(100))
		random := 0
		outData := fmt.Sprintf("RTN-%s-%s-%s:%d [%d]", command.Data,
			command.JobInfo.Config["target"], runOptions.Name, random, command.CommandCnt)

		// time.Sleep(time.Duration(random) * time.Millisecond)
		return outData
	}

	factory.RegisterHandler("echo", handler)

	daemonizer.RegisterWorkerFactory(factory)
	disp := dispatch.New(daemonizer.GetKernel())
	dispatch.SupportAPI(disp, daemonizer.GetAPIServer())

	daemonizer.Start()

	{
		job1 := job.NewWithPIAndID("job1", "dispatch", `{"handler":"echo", "config":{"target":"A"}}`)
		daemonizer.AddJobIfNotExists(job1)

		job2 := job.NewWithPIAndID("job2", "dispatch", `{"handler":"echo", "config":{"target":"B"}}`)
		daemonizer.AddJobIfNotExists(job2)

		job3 := job.NewWithPIAndID("job3", "dispatch", `{"handler":"echo", "config":{"target":"C"}}`)
		daemonizer.AddJobIfNotExists(job3)

		job4 := job.NewWithPIAndID("job4", "dispatch", `{"handler":"echo", "config":{"target":"D"}}`)
		daemonizer.AddJobIfNotExists(job4)

		job5 := job.NewWithPIAndID("job5", "dispatch", `{"handler":"echo", "config":{"target":"E"}}`)
		daemonizer.AddJobIfNotExists(job5)

		job6 := job.NewWithPIAndID("job6", "dispatch", `{"handler":"echo", "config":{"target":"F"}}`)
		daemonizer.AddJobIfNotExists(job6)

		job7 := job.NewWithPIAndID("job7", "dispatch", `{"handler":"echo", "config":{"target":"F"}}`)
		daemonizer.AddJobIfNotExists(job7)
	}

	daemonizer.Wait()
}
