package main

import (
	"bytes"
	"container/ring"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const connectionLimit = 200

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

func (counter *Counter) sub() int64 {
	counter.Lock()
	counter.count--
	// fmt.Println("**** ***** ******* sub cnt:", counter.count)
	counter.Unlock()
	return counter.count
}

func main() {
	reqCounter := new(Counter)
	respCounter := new(Counter)
	connCounter := new(Counter)
	jobRing := ring.New(8)

	for i := 1; i < 8; i++ {
		jobRing = jobRing.Next()
		jobRing.Value = fmt.Sprintf("job%d", i)
		// jobRing = jobRing.Next()
		// jobRing.Value = fmt.Sprintf("job%d", i)
	}

	jobRing = jobRing.Next()
	jobRing.Value = "job2"
	// jobRing = jobRing.Next()
	// jobRing.Value = "job2"

	portRing := ring.New(3)

	for i := 5; i < 8; i++ {
		portRing = portRing.Next()
		portRing.Value = fmt.Sprintf("1234%d", i)
	}

	start := time.Now()

	for reqCount := 0; reqCount < 100; reqCount++ {
		numStr := strconv.Itoa(reqCount)

		for i := 0; i < 7; i++ {
			jobRing = jobRing.Next()
			portRing = portRing.Next()
			go putData(jobRing.Value.(string), portRing.Value.(string), numStr, connCounter, reqCounter, respCounter)
		}

		time.Sleep(100 * time.Millisecond)
	}

	for reqCounter.count > respCounter.count {
		time.Sleep(100 * time.Millisecond)
	}

	end := time.Now()
	ellapsed := end.Sub(start)
	avg := int64(ellapsed) / reqCounter.count
	fmt.Printf("@ All Request: %d [%s] avg.=%s\n", reqCounter.count, ellapsed, time.Duration(avg))
}

func putData(jobid string, port string, numStr string, connCounter, reqCounter, respCounter *Counter) {
	reqCount := reqCounter.add()

	body := fmt.Sprintf("%s-%s [%d]", strings.ToUpper(jobid), numStr, reqCount)

	reqBody := bytes.NewBufferString(body)

	for connCounter.count > connectionLimit {
		fmt.Println("* Sleep ", reqCount, ", conn:", connCounter.count)
		time.Sleep(100 * time.Millisecond)
	}

	start := time.Now()
	fmt.Printf("@ Put: %s to %s (%s) [%d]\n", jobid, port, numStr, reqCount)

	url := fmt.Sprintf("http://127.0.0.1:%s/api/v1/dispatch/post/%s", port, jobid)

	connCounter.add()
	defer connCounter.sub()

	resp, err := http.Post(url, "text/plain", reqBody)

	respCount := respCounter.add()

	if err != nil {
		log.Println("[ERROR] Put http >", err)
		return
	}

	defer resp.Body.Close()

	end := time.Now()
	ellapsed := end.Sub(start)
	// Response 체크.
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("[ERROR] read http response >", err)
		return
	}

	fmt.Printf("@ Resp: %s resp=%s [%d]:[%d] %s\n", jobid, string(respBody), reqCount, respCount, ellapsed)
}
