package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	nReduce          int
	files            []string
	finishedMaps     []bool
	finishedReduces  []bool
	totalMaps        int
	mapsDone         chan struct{} // Once all maps done, permanently full
	remainingMaps    chan int
	remainingReduces chan int
}

// Your code here -- RPC handlers for the worker to call.

//
// Workers will query the scheduler to figure out what they should do.
// This will block until there is an appropriate task for the worker.
//
func (c *Coordinator) Schedule(args *ScheduleArgs, reply *ScheduleReply) error {
	// TODO: Schedule checks for if these are finished
	reply.NReduce = c.nReduce
	select {
	case <-c.mapsDone:
		// Reduce
		workerNum := <-c.remainingReduces
		c.mapsDone <- struct{}{}

		// input flies are known at this point
		inputFiles := make([]string, c.totalMaps)
		for i := range inputFiles {
			inputFiles[i] = fmt.Sprintf("mr-%d-%d", i, workerNum)
		}
		reply.IsReduce = true
		reply.InputFiles = inputFiles
		reply.WorkerNum = workerNum
	case workerNum := <-c.remainingMaps:
		// Map
		reply.IsReduce = false
		reply.InputFiles = []string{c.files[workerNum]}
		reply.WorkerNum = workerNum
	}
	return nil
}

func areAllDone(finished []bool) bool {
	for _, done := range finished {
		if !done {
			return false
		}
	}
	return true
}

func (c *Coordinator) Completion(args *CompletionArgs, reply *CompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.IsReduce {
		c.finishedReduces[args.WorkerNum] = true
	} else {
		wasDone := areAllDone((c.finishedMaps))
		c.finishedMaps[args.WorkerNum] = true
		// Check if this is the first instance where maps are all done
		if areAllDone(c.finishedMaps) && !wasDone {
			c.mapsDone <- struct{}{}
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := areAllDone(c.finishedReduces)

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	remainingMaps := make(chan int, len(files))
	remainingReduces := make(chan int, len(files))
	mapsDone := make(chan struct{})
	// Initially, everything still has to be done
	for i := range files {
		remainingMaps <- i
		remainingReduces <- i
	}
	c := Coordinator{
		nReduce:          nReduce,
		files:            files,
		finishedMaps:     make([]bool, len(files)),
		finishedReduces:  make([]bool, nReduce),
		totalMaps:        len(files),
		mapsDone:         mapsDone,
		remainingMaps:    remainingMaps,
		remainingReduces: remainingReduces,
	}

	// Your code here.

	c.server()
	return &c
}
