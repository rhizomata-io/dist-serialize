package main

import "time"

func main() {
	var finish chan bool = make(chan bool)

	go func(finish chan bool) {
		time.Sleep(2 * time.Second)
		finish <- true
	}(finish)

	<-finish
}
