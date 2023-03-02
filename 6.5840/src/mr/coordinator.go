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
	lock                      sync.Mutex
	stage                     string
	map_num                   int
	reduce_num                int
	tasks                     map[string]Task
	internal_file             map[string][]string
	available_tasks           chan Task
	task_finished_map         map[int]bool
	task_finished_num         int
	finished_reducce_task     map[int]bool
	finished_reducce_task_num int
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
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.reduce_num == c.finished_reducce_task_num {
		log.Println("the master exit, all task is finished")
		os.Exit(0)
		return true
	}
	// Your code here.
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:                     "Map",
		map_num:                   len(files),
		reduce_num:                nReduce,
		tasks:                     make(map[string]Task),
		internal_file:             make(map[string][]string),
		available_tasks:           make(chan Task, int(len(files))),
		task_finished_map:         make(map[int]bool),
		task_finished_num:         0,
		finished_reducce_task:     make(map[int]bool),
		finished_reducce_task_num: 0,
	}
	for _, file := range files {
		task := Task{
			task_type:       "Map",
			file_name:       file,
			available_tasks: c.available_tasks,
		}
		c.tasks[file] = task
		c.available_tasks <- task
	}

	// Your code here.

	c.server()
	go func() {
		for {
			c.lock.Lock()
			defer c.lock.Unlock()
			if c.stage == "Reduce" {
				map_reduce_bucket := make(map[int][]key_file)
				for key, files := range c.internal_file {
					reduce_id := ihash(key) % c.reduce_num
					item_to_append := key_file{
						Key:   key,
						files: files,
					}
					map_reduce_bucket[reduce_id] = append(map_reduce_bucket[reduce_id], item_to_append)
				}
				for i, this_key_file := range map_reduce_bucket {
					c.available_tasks <- Task{
						task_type:       "Reduce",
						available_tasks: c.available_tasks,
						num_reduce:      c.reduce_num,
						task_id:         i,
						key_files:       this_key_file,
					}
				}
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
	return &c
}

func (c *Coordinator) ApplyForTask(args *RpcArgs, replys *RpcReply) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if args.task_finished {
		// conflict let the worker exit
		if args.type_request != c.stage {
			log.Println("the stage and task conflict")
			replys.conflict = true
		}
		// map task finish, update the state
		if args.type_request == "Map" {
			c.task_finished_map[args.task_id] = true
			c.task_finished_num++
			for key, files := range args.internal_file {
				c.internal_file[key] = append(c.internal_file[key], files...)
			}
			if c.task_finished_num == c.map_num {
				c.stage = "Reduce"
			}
			return
		}
		// reduce task finish, update the state
		if args.type_request == "Reduce" {
			c.finished_reducce_task[args.task_id] = true
			c.finished_reducce_task_num++
			return
		}
	}
	if !args.task_finished {
		if c.stage == "Map" {
			replys.task.task_type = "Map"
			replys.task = <-c.available_tasks
			replys.is_success = true
			return
		}
		if c.stage == "Reduce" {
			replys.task.task_type = "Reduce"
			replys.task = <-c.available_tasks
			replys.is_success = true
			return
		}
	}
}
