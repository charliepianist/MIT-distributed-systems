package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ScheduleArgs struct {
}

type ScheduleReply struct {
	IsReduce   bool     // Map if false, Reduce if true
	InputFiles []string // just the "split" for map, otherwise all map workers' outputs
	NReduce    int
	WorkerNum  int // Which worker is this? 0 < workerNum < nReduce for reduce
}

type CompletionArgs struct {
	IsReduce  bool
	WorkerNum int
}

type CompletionReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
