package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrint(v ...interface{}) (n int, err error) {
	if Debug {
		log.Print(v...)
	}
	return
}

type Coordinator struct {
	// Your definitions here.
	nReduce           int
	pendingMapTasks   map[int]string   // id: file name
	runningMapTasks   map[int]string   // id: file name
	completedMapTasks map[int][]string // id: intermediate file names
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	var currFile string
	var mapTaskId int

	// should be in mutex lock

	if len(c.pendingMapTasks) > 0 {
		for id, fileName := range c.pendingMapTasks {
			mapTaskId, currFile = id, fileName
			break
		}
		delete(c.pendingMapTasks, mapTaskId)
		c.runningMapTasks[mapTaskId] = currFile
		DPrintf("sent file %s\n", currFile)
	} else {
		DPrintf("no files to send")
	}

	reply.Filename = currFile
	reply.NReduce = c.nReduce
	reply.MapTaskID = mapTaskId
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	DPrintf("server listening at %v", sockname)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	c := Coordinator{
		nReduce:           nReduce,
		pendingMapTasks:   make(map[int]string),
		runningMapTasks:   make(map[int]string),
		completedMapTasks: make(map[int][]string),
	}

	for idx, fileNames := range files {
		c.pendingMapTasks[idx] = fileNames
	}

	c.server()
	return &c
}
