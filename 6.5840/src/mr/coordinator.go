package mr

import (
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
	lock            sync.Mutex
	stage           string
	map_num         int
	reduce_num      int
	tasks           map[string]Task
	available_tasks chan Task
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
		stage:           "Map",
		map_num:         len(files),
		reduce_num:      nReduce,
		tasks:           make(map[string]Task),
		available_tasks: make(chan Task, int(len(files))),
	}
	for _, file := range files {
		task := Task{}
		c.tasks[file] = task
		c.available_tasks <- task
	}
	log.Println("Coordinator start work")

	// Your code here.

	c.server()
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {

				// to do
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

func (c *Coordinator) ApplyForTask(args *RpcArgs, replys *RpcReply) {
}
