package mr

import "log"
import "sync"
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

	mutex                sync.Mutex
	inputSplits          []string
	nReduce              int
	pendingMapTasks      map[int]string   // map task's id: input split file name
	runningMapTasks      map[int]string   // map task's id: input split file name
	completedMapTasks    map[int][]string // map task's id: intermediate file names
	pendingReduceTasks   map[int][]string // reduce task's id: intermediate file names
	runningReduceTasks   map[int][]string // reduce task's id: intermediate file names
	completedReduceTasks map[int]string   // reduce task's id: output file name
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	currFile, mapTaskId := c.getPendingMapTask()

	reply.Filename = currFile
	reply.NReduce = c.nReduce
	reply.MapTaskID = mapTaskId
	return nil
}

func (c *Coordinator) InformMapTaskResult(args *InformMapTaskResultArgs, reply *InformMapTaskResultReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for fileName, reduceTaskId := range args.IntermediateFileNames {
		c.completedMapTasks[args.TaskID] = append(c.completedMapTasks[args.TaskID], fileName)
		c.pendingReduceTasks[reduceTaskId] = append(c.pendingReduceTasks[reduceTaskId], fileName)
	}
	delete(c.runningMapTasks, args.TaskID)
	if c.checkForMapTask() {
		reply.Action = GetMapTaskAction
	} else if c.checkForReduceTask() {
		reply.Action = GetReduceTaskAction
	} else {
		reply.Action = ShutDownAction
	}

	return nil
}

func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	currFiles, reduceTaskId := c.getPendingReduceTask()

	reply.Filenames = currFiles
	reply.ReduceTaskID = reduceTaskId
	return nil
}

func (c *Coordinator) InformReduceTaskResult(args *InformReduceTaskResultArgs, reply *InformReduceTaskResultReply) error {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.completedReduceTasks[args.TaskID] = args.OutputFilename
	delete(c.runningReduceTasks, args.TaskID)
	if c.checkForReduceTask() {
		reply.Action = GetReduceTaskAction
	} else {
		reply.Action = ShutDownAction
	}

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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.completedReduceTasks) == c.nReduce
}

func (c *Coordinator) getPendingMapTask() (string, int) {
	var currFile string
	var mapTaskId int

	//DPrint("pending map tasks ", c.pendingMapTasks)
	//DPrint("running map tasks ", c.runningMapTasks)
	//DPrint("completed map tasks ", c.completedMapTasks)
	//DPrint("pending reduce tasks ", c.pendingReduceTasks)

	if len(c.pendingMapTasks) > 0 {
		for id, fileName := range c.pendingMapTasks {
			mapTaskId, currFile = id, fileName
			break
		}
		delete(c.pendingMapTasks, mapTaskId)
		c.runningMapTasks[mapTaskId] = currFile
		DPrintf("sent map task %d with file %s\n", mapTaskId, currFile)
		return currFile, mapTaskId
	} else if len(c.runningMapTasks) > 0 {
		for id, fileName := range c.runningMapTasks {
			mapTaskId, currFile = id, fileName
			break
		}
		DPrintf("sent (running) map task %d with file %s\n", mapTaskId, currFile)
		return currFile, mapTaskId
	} else {
		return "", -1
	}

}

func (c *Coordinator) checkForMapTask() bool {
	return len(c.pendingMapTasks) > 0 || len(c.runningMapTasks) > 0
}

func (c *Coordinator) areMapTasksCompleted() bool {
	return len(c.completedMapTasks) == len(c.inputSplits)
}

func (c *Coordinator) getPendingReduceTask() ([]string, int) {
	var currFiles = make([]string, c.nReduce)
	var reduceTaskId int

	if len(c.pendingReduceTasks) > 0 {
		for id, fileNames := range c.pendingReduceTasks {
			reduceTaskId, currFiles = id, fileNames
			break
		}
		delete(c.pendingReduceTasks, reduceTaskId)
		c.runningReduceTasks[reduceTaskId] = currFiles
		DPrintf("sent reduce task %d with files %s\n", reduceTaskId, currFiles)
		return currFiles, reduceTaskId
	} else if len(c.runningReduceTasks) > 0 {
		for id, fileNames := range c.runningReduceTasks {
			reduceTaskId, currFiles = id, fileNames
			break
		}
		DPrintf("sent (running) reduce task %d with file %s\n", reduceTaskId, currFiles)
		return currFiles, reduceTaskId
	} else {
		return make([]string, 0), -1
	}

}

func (c *Coordinator) checkForReduceTask() bool {
	return len(c.pendingReduceTasks) > 0 || len(c.runningReduceTasks) > 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	c := Coordinator{
		inputSplits:          files,
		nReduce:              nReduce,
		pendingMapTasks:      make(map[int]string),
		runningMapTasks:      make(map[int]string),
		completedMapTasks:    make(map[int][]string),
		pendingReduceTasks:   make(map[int][]string),
		runningReduceTasks:   make(map[int][]string),
		completedReduceTasks: make(map[int]string),
	}

	for idx, fileNames := range files {
		c.pendingMapTasks[idx] = fileNames
	}

	c.server()
	return &c
}
