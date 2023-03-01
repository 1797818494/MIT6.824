package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock           sync.Mutex
	stage          string
	nMap           int
	tasks          map[string]Task
	availableTasks chan Task
}

// Your code here -- RPC handlers for the worker to call.

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
	ret := false
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:          "Map",
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(len(files))),
	}
	for i, file := range files {
		task := Task{
			taskType:     "Map",
			taskid:       i,
			mapInputFile: file,
		}
		c.tasks[file] = task
		c.availableTasks <- task
	}
	log.Println("Coordinator start work")

	// Your code here.

	c.server()
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.workerId != "" && time.Now().After(task.deadline) {
					fmt.Printf("Found timed-out %s task %d previously running on worker %s. Prepare to re-assign",
						task.taskType, task.taskid, task.workerId)
				}
				task.workerId = ""
				c.availableTasks <- task
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

func (c *Coordinator) ApplyForTask()
