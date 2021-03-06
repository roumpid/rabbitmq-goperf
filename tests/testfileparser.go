package tests

import (
	"time"

	"github.com/roumpid/rabbitmq-goperf/services/fileparser"
)

// PerfTestParseDocument ..
func PerfTestParseDocument() {
	r := fileparser.Reply{}
	r.CreateQueue()
	for {
		r.Parse()
		time.Sleep(100 * time.Millisecond)
	}
}
