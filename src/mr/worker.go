package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
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
	for filename, nReduce, mapTaskId := GetMapTask(); filename != ""; {
		MapWorker(mapf, filename, nReduce, mapTaskId)
		filename, nReduce, mapTaskId = GetMapTask()
	}
	DPrint("all map tasks are completed")

	for fileNames, reduceTaskId := GetReduceTask(); len(fileNames) > 0; {
		ReduceWorker(reducef, fileNames, reduceTaskId)
		fileNames, reduceTaskId = GetReduceTask()
	}
	DPrint("all reduce tasks completed")

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
		DPrint("dialing:", err)
		DPrint("master is offline")
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func MapWorker(mapf func(string, string) []KeyValue, filename string, nReduce int, mapTaskId int) string {

	if filename == "" {
		DPrint("no split received, shutting down")
		return ShutDownAction
	}

	kva := GetIntermediatePairs(mapf, filename)

	intermediateFileNames := make(map[string]int)
	intermediateTempFiles := make([]*os.File, nReduce)
	intermediatePrefix := fmt.Sprintf("mr-%d", mapTaskId)

	DPrint("processing ", mapTaskId, " ", filename)

	for idx := 0; idx < nReduce; idx++ {
		pattern := fmt.Sprintf("%s-%d-*", intermediatePrefix, idx)
		intermediateTempFiles[idx] = GetTempFile(pattern)
	}

	for _, kv := range kva {
		Y := ihash(kv.Key) % nReduce
		intermediateFilename := fmt.Sprintf("%s-%d", intermediatePrefix, Y)
		PutKvInIntermediateFile(kv, intermediateTempFiles[Y])
		intermediateFileNames[intermediateFilename] = Y
	}

	for idx := 0; idx < nReduce; idx++ {
		intermediateFileName := fmt.Sprintf("%s-%d", intermediatePrefix, idx)
		// check if file exists
		if _, err := os.Stat(intermediateFileName); err != nil {
			os.Rename(intermediateTempFiles[idx].Name(), intermediateFileName)
		}
	}

	// handle multiple inform retries exponentially later
	result, _ := InformMapTaskResult(mapTaskId, intermediateFileNames)
	return result
}

func GetMapTask() (string, int, int) {
	args := GetMapTaskArgs{}
	reply := GetMapTaskReply{}

	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		if reply.Filename != "" {
			DPrintf("Got file %v, starting map task\n", reply.Filename)
		} else {
			DPrintf("No files atm\n")
		}
		//DPrint(reply)
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
	//DPrint("GetIntermediatePairs ", len(kva), kva[0])
	return kva
}

func InformMapTaskResult(
	mapTaskId int,
	intermediateFiles map[string]int,
) (string, error) {
	args := InformMapTaskResultArgs{
		mapTaskId,
		intermediateFiles,
	}
	reply := InformMapTaskResultReply{}

	ok := call("Coordinator.InformMapTaskResult", &args, &reply)
	if ok {
		return reply.Action, nil
	} else {
		DPrint("InformMapTaskResult call failed!")
		return "", nil
	}
}

func PutKvInIntermediateFile(kv KeyValue, file *os.File) {
	intermediateFile, err := os.OpenFile(file.Name(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("os.OpenFile", err)
	}
	defer intermediateFile.Close()

	enc := json.NewEncoder(intermediateFile)
	err = enc.Encode(&kv)
	if err != nil {
		log.Fatalf("cannot encode %v", file.Name())
	}
}

func ReduceWorker(reducef func(string, []string) string, fileNames []string, reduceTaskId int) string {

	if len(fileNames) == 0 {
		DPrint("no files received, shutting down")
		return ShutDownAction
	}

	intermediate := make([]KeyValue, 0)

	for _, fileName := range fileNames {
		GetKVFromIntermediateFile(fileName, &intermediate)
	}

	// fill intermediates

	sort.Sort(ByKey(intermediate))

	outFileName := fmt.Sprintf("mr-out-%d", reduceTaskId)
	pattern := fmt.Sprintf("%s-*", outFileName)
	tempOutFile := GetTempFile(pattern)
	defer tempOutFile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempOutFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	// check if file exists
	if _, err := os.Stat(outFileName); err != nil {
		os.Rename(tempOutFile.Name(), outFileName)
	}
	result, _ := InformReduceTaskResult(reduceTaskId, outFileName)
	return result
}

func GetReduceTask() ([]string, int) {
	args := GetReduceTaskArgs{}
	reply := GetReduceTaskReply{}

	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {
		if len(reply.Filenames) != 0 {
			DPrintf("Got files %v, starting reduce task\n", reply.Filenames)
			return reply.Filenames, reply.ReduceTaskID
		} else {
			DPrintf("No files atm\n")
			return reply.Filenames, reply.ReduceTaskID
		}
		//DPrint(reply)
	}

	DPrint("GetReduceTask call failed!")
	return reply.Filenames, -1
}

func InformReduceTaskResult(
	mapTaskId int,
	outputFile string,
) (string, error) {
	args := InformReduceTaskResultArgs{
		mapTaskId,
		outputFile,
	}
	reply := InformReduceTaskResultReply{}

	ok := call("Coordinator.InformReduceTaskResult", &args, &reply)
	if ok {
		return reply.Action, nil
	} else {
		DPrint("InformReduceTaskResult call failed!")
		return "", nil
	}
}

func GetKVFromIntermediateFile(filename string, intermediate *[]KeyValue) {

	file, _ := os.Open(filename)
	defer file.Close()

	//var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		*intermediate = append(*intermediate, kv)
	}
	//return kva
}

func GetTempFile(pattern string) *os.File {
	pwd, _ := os.Getwd()
	ofile, _ := os.CreateTemp(pwd, pattern)
	return ofile
}
