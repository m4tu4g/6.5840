package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	MapWorker(mapf)

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func MapWorker(mapf func(string, string) []KeyValue) {

	filename, nReduce, mapTaskId := GetMapTask()
	if filename == "" {
		DPrint("no split received, shutting down")
		return
	}

	kva := GetIntermediatePairs(mapf, filename)

	intermediateFileNames := make(map[string]bool)
	DPrint("processing ", mapTaskId, filename)
	for _, kv := range kva {
		Y := ihash(kv.Key) % nReduce
		intermediateFilename := fmt.Sprintf("map-%d-%d", mapTaskId, Y)
		PutKvInIntermediateFile(kv, intermediateFilename)
		intermediateFileNames[intermediateFilename] = true
	}

	// convert map's keys into array
	intermediateFileNamesArray := make([]string, len(intermediateFileNames))
	i := 0
	for k := range intermediateFileNames {
		intermediateFileNamesArray[i] = k
		i++
	}

	// handle multiple inform retries exponentially later
	InformMapTaskResult(mapTaskId, intermediateFileNamesArray, mapf)

}

func ReduceWorker(mapf func(string, []string) string, reducef func(string, []string) string) {

}

func GetMapTask() (string, int, int) {
	args := GetMapTaskArgs{}
	reply := GetMapTaskReply{}

	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		if reply.Filename != "" {
			fmt.Printf("Got file %v, starting map task\n", reply.Filename)
		} else {
			fmt.Printf("No files atm\n")
		}
		DPrint(reply)
		return reply.Filename, reply.NReduce, reply.MapTaskID
	}

	DPrint("GetMapTask call failed!")
	return "", 0, -1
}

func GetIntermediatePairs(mapf func(string, string) []KeyValue, filename string) []KeyValue {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	kva := mapf(filename, string(content))
	DPrint("GetIntermediatePairs ", len(kva), kva[0])
	return kva
}

func InformMapTaskResult(mapTaskId int, intermediateFiles []string, mapf func(string, string) []KeyValue) error {
	args := InformMapTaskResultArgs{
		mapTaskId,
		intermediateFiles,
	}
	reply := InformMapTaskResultReply{}

	ok := call("Coordinator.InformMapTaskResult", &args, &reply)
	if ok {
		if reply.Action == GetTaskAction {
			MapWorker(mapf)
		} else if reply.Action == ShutDownAction {
			DPrint("shut down action received")
		}
	} else {
		DPrint("InformMapTaskResult call failed!")
	}
	return nil
}

func PutKvInIntermediateFile(kv KeyValue, file string) {
	intermediateFile, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("os.OpenFile", err)
	}
	defer intermediateFile.Close()

	enc := json.NewEncoder(intermediateFile)
	err = enc.Encode(&kv)
	if err != nil {
		log.Fatalf("cannot encode %v", file)
	}
}
