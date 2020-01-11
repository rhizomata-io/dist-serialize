package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
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
		factory := &dispatch.Factory{}
		daemonizer.RegisterWorkerFactory(factory)
		// daemonizer.Start()
	} else {
		log.Fatal("ERROR", err)
	}

	job1 := job.NewWithPIAndID("job1", "dispatch", `{"init":"helloA"}`)
	daemonizer.AddJobIfNotExists(job1)

	job2 := job.NewWithPIAndID("job2", "dispatch", `{"init":"helloB"}`)
	daemonizer.AddJobIfNotExists(job2)

	job3 := job.NewWithPIAndID("job3", "dispatch", `{"init":"helloC"}`)
	daemonizer.AddJobIfNotExists(job3)

	disp := dispatch.New(daemonizer.GetKernel())

	counter1 := new(Counter)
	counter2 := new(Counter)
	counter3 := new(Counter)
	for i := 0; i < 500; i++ {
		numStr := strconv.Itoa(i)
		go putData(disp, daemonizer.GetKernel().ID(), "job1", numStr, counter1)
		go putData(disp, daemonizer.GetKernel().ID(), "job2", numStr, counter2)
		go putData(disp, daemonizer.GetKernel().ID(), "job3", numStr, counter3)
	}
	daemonizer.Wait()
	// sigs := make(chan os.Signal, 1)
	// signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// <-sigs
}

// Counter ..
type Counter struct {
	sync.Mutex
	count int64
}

func (counter *Counter) add() int64 {
	counter.Lock()
	counter.count++
	counter.Unlock()
	return counter.count
}

func putData(disp *dispatch.Dispatch, kernelid string, jobid string, numStr string, counter *Counter) {
	start := time.Now()
	resp, _ := disp.Put(jobid, fmt.Sprintf("%s-%s-%s", strings.ToUpper(jobid), kernelid, numStr))
	end := time.Now()
	ellapsed := end.Sub(start)

	fmt.Printf("@ Resp: %s resp=%s [%d] %s\n", jobid, string(resp), counter.add(), ellapsed)
}
