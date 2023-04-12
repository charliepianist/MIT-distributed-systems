package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// Figure out what this worker should do
	reply := ScheduleReply{}
	CallSchedule(&reply)

	if reply.IsReduce {
		kva := make([]KeyValue, 10)
		for _, filename := range reply.InputFiles {
			// Decode file
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}

		// Sort key-value pairs by key
		sort.Sort(ByKey(kva))

		// Create temp file
		file, err := ioutil.TempFile("", fmt.Sprintf("reduce-%v", reply.WorkerNum))
		if err != nil {
			log.Fatalf("Could not create temp file for worker %v", reply.WorkerNum)
		}

		// write to temp file
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				// IMPORTANT: dedupe (intermediate files may have duplicates on crashes)
				if k > i && kva[k-1].Value == kva[k].Value {
					continue
				}
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
			i = j
		}
		// Move to permanent
		permanent_filename := fmt.Sprintf("mr-out-%d", reply.WorkerNum)
		err = os.Rename(file.Name(), permanent_filename)
		if err != nil {
			log.Fatalf("Failed to rename %v to %v", file.Name(), permanent_filename)
		}

		// Inform coordinator that we have finished
		CallCompletion(true, reply.WorkerNum)
	} else {
		// Map
		// Read file, apply map function
		inputFile := reply.InputFiles[0]
		file, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("Cannot open %v", inputFile)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("Cannot read %v", inputFile)
		}
		file.Close()
		kva := mapf(inputFile, string(content))

		// Save to file
		encoders := make([]*json.Encoder, reply.NReduce)
		// Open all files
		for i := range encoders {
			filename := fmt.Sprintf("mr-%d-%d", reply.WorkerNum, i)
			file, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("Cannot open file %v", filename)
			}
			defer file.Close()
			encoders[i] = json.NewEncoder(file)
		}
		// Write to files
		for _, keyValue := range kva {
			key := keyValue.Key
			hash := ihash(key) % reply.NReduce
			encoder := encoders[hash]
			err := encoder.Encode(&keyValue)
			if err != nil {
				log.Fatalf("Failed to write %v to %v", keyValue, encoder)
			}
		}

		CallCompletion(false, reply.WorkerNum)
	}
}

func CallSchedule(reply *ScheduleReply) {
	args := ScheduleArgs{}
	ok := call("Coordinator.Schedule", &args, reply)
	if !ok {
		fmt.Printf("Call to schedule failed!\n")
	}
}

func CallCompletion(isReduce bool, workerNum int) {
	args := CompletionArgs{IsReduce: isReduce, WorkerNum: workerNum}
	reply := CompletionReply{}
	ok := call("Coordinator.Completion", &args, reply)
	if !ok {
		fmt.Printf("Call to completion failed!\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
