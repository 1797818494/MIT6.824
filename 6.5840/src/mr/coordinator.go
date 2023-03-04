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
	Lock                      sync.Mutex
	Stage                     string
	Map_num                   int
	Reduce_num                int
	Tasks                     map[string]Task
	Internal_file             map[string][]string
	Map_tasks_chan            chan Task
	Reduce_tasks_chan         chan Task
	Task_finished_map         map[int]bool
	Task_finished_num         int
	Finished_reducce_task     map[int]bool
	Finished_reducce_task_num int
	Map_timer                 map[int]time.Time
	Reduce_timer              map[int]time.Time
	Map_Task                  map[int]Task
	Reduce_Task               map[int]Task
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
	c.Lock.Lock()
	defer c.Lock.Unlock()
	if c.Reduce_num == c.Finished_reducce_task_num {
		log.Println("the master exit, all task is finished")
		os.Exit(0)
		return true
	}
	// Your code here.
	return false
}

type Key_and_files_set struct {
	Key   map[string]bool
	Files map[string]bool
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Stage:                     "Map",
		Map_num:                   len(files),
		Reduce_num:                nReduce,
		Tasks:                     make(map[string]Task),
		Internal_file:             make(map[string][]string),
		Map_tasks_chan:            make(chan Task, int(len(files))),
		Reduce_tasks_chan:         make(chan Task, nReduce),
		Task_finished_map:         make(map[int]bool),
		Task_finished_num:         0,
		Finished_reducce_task:     make(map[int]bool),
		Finished_reducce_task_num: 0,
		Map_timer:                 make(map[int]time.Time),
		Reduce_timer:              make(map[int]time.Time),
		Map_Task:                  make(map[int]Task),
		Reduce_Task:               make(map[int]Task),
	}
	for i, file := range files {
		log.Println(i)
		task := Task{
			Task_type:  "Map",
			File_name:  file,
			Task_id:    i,
			Num_reduce: c.Reduce_num,
			IsPush:     false,
		}
		// c.Map_Task[i] = task
		c.Map_tasks_chan <- task
	}
	log.Println("start server")

	// Your code here.

	c.server()

	go func() {
		for {
			time.Sleep(10 * time.Second)
			c.Lock.Lock()
			log.Println("start dead task check")
			if nReduce == c.Finished_reducce_task_num {
				c.Lock.Unlock()
				break
			}
			for id, this_time := range c.Map_timer {
				if c.Map_Task[id].IsPush && !c.Task_finished_map[id] && time.Now().After(this_time.Add(10*time.Second)) {
					log.Println("the map is crash!!!!!!!!!")
					c.Lock.Unlock()
					c.Map_tasks_chan <- c.Map_Task[id]
					c.Lock.Lock()
					delete(c.Map_Task, id)
				}
			}
			log.Println(c.Reduce_timer)
			for id, this_time := range c.Reduce_timer {
				log.Println(c.Reduce_Task[id].IsPush, c.Finished_reducce_task[id])
				if c.Reduce_Task[id].IsPush && !c.Finished_reducce_task[id] && time.Now().After(this_time.Add(10*time.Second)) {
					log.Println("the reduce is crash!!!!!!!!!", id)
					c.Lock.Unlock()
					c.Reduce_tasks_chan <- c.Reduce_Task[id]
					c.Lock.Lock()
					delete(c.Reduce_Task, id)
				}
			}
			c.Lock.Unlock()
			log.Println("start dead task check finish")
		}
	}()

	go func() {
		for {
			c.Lock.Lock()
			if c.Stage == "Reduce" {
				map_reduce_bucket := make(map[int]Key_and_files_set)
				for i := 0; i < c.Reduce_num; i++ {
					map1 := make(map[string]bool)
					map2 := make(map[string]bool)
					map_reduce_bucket[i] = Key_and_files_set{Key: map1, Files: map2}
				}
				for key, files := range c.Internal_file {
					reduce_id := ihash(key) % c.Reduce_num
					map_reduce_bucket[reduce_id].Key[key] = true
					for _, file := range files {
						map_reduce_bucket[reduce_id].Files[file] = true
					}

				}
				for i, Key_and_files := range map_reduce_bucket {
					log.Println("reduce task produce ", i)
					// log.Println(this_key_file)
					c.Reduce_tasks_chan <- Task{
						Task_type:  "Reduce",
						Num_reduce: c.Reduce_num,
						Task_id:    i,
						Files_set:  Key_and_files,
						IsPush:     false,
					}
					// c.Reduce_Task[i] = Task{
					// 	Task_type:  "Reduce",
					// 	Num_reduce: c.Reduce_num,
					// 	Task_id:    i,
					// 	Files_set:  Key_and_files,
					// 	IsPush:     true,
					// }
				}
				c.Lock.Unlock()
				break
			}
			c.Lock.Unlock()
			time.Sleep(1000 * time.Millisecond)
		}
	}()
	return &c
}

func (c *Coordinator) ApplyForTask(args *RpcArgs, replys *RpcReply) error {
	c.Lock.Lock()
	log.Println("RPC Apply")
	if args.Task_finished {
		log.Println("receive reply")
		// conflict let the worker exit
		if args.Type_request != c.Stage || (c.Stage == "Map" && c.Task_finished_map[args.Task_id]) || (c.Stage == "Reduce" && c.Finished_reducce_task[args.Task_id]) {
			log.Println("the stage and task conflict")
			replys.Conflict = true
			c.Lock.Unlock()
			return nil
		}
		// map task finish, update the state
		if args.Type_request == "Map" {
			c.Task_finished_map[args.Task_id] = true
			c.Task_finished_num++
			log.Println("task num and map num", c.Task_finished_num, c.Map_num, args.Task_id)
			for key, files := range args.Internal_file {
				// log.Println(files)
				c.Internal_file[key] = append(c.Internal_file[key], files...)
			}
			if c.Task_finished_num == c.Map_num {
				log.Println("stage change Reduce")
				c.Stage = "Reduce"
			}
			c.Lock.Unlock()
			return nil
		}
		// reduce task finish, update the state
		if args.Type_request == "Reduce" {
			c.Finished_reducce_task[args.Task_id] = true
			c.Finished_reducce_task_num++
			log.Println("reduce finished task", c.Finished_reducce_task_num, args.Task_id)
			c.Lock.Unlock()
			return nil
		}
	}
	if !args.Task_finished {
		if c.Stage == "Map" {
			replys.Task.Task_type = "Map"
			c.Lock.Unlock()
			log.Println("the block is ", c.Task_finished_num)
			select {
			case replys.Task = <-c.Map_tasks_chan:
			case <-time.After(10 * time.Second): // 超时10秒没有获得数据，则退出程序，如果只是退出循环，可以return改为continue
				log.Println("Map !! More than 10 second no input, return!!!!!!!!!")
				replys.Conflict = true
				return nil
			}
			log.Println("here1")
			c.Lock.Lock()
			log.Println("reach here")
			c.Map_timer[replys.Task.Task_id] = time.Now()
			replys.Task.IsPush = true
			c.Map_Task[replys.Task.Task_id] = replys.Task
			c.Lock.Unlock()

			log.Println("the finished block is ", c.Task_finished_num)
			replys.Is_success = true
			return nil
		}
		if c.Stage == "Reduce" {
			log.Println("Reduce dispatch")
			replys.Task.Task_type = "Reduce"
			c.Lock.Unlock()
			select {
			case replys.Task = <-c.Reduce_tasks_chan:
			case <-time.After(10 * time.Second): // 超时10秒没有获得数据，则退出程序，如果只是退出循环，可以return改为continue
				log.Println("More than 10 second no input, return!!!!!!!!!")
				replys.Conflict = true
				return nil
			}

			log.Println("here1!")
			c.Lock.Lock()
			log.Println("reach here!!")
			log.Println("here is task id", replys.Task.Task_id)
			replys.Task.IsPush = true
			c.Reduce_timer[replys.Task.Task_id] = time.Now()
			c.Reduce_Task[replys.Task.Task_id] = replys.Task
			log.Println("the finished block is ", c.Finished_reducce_task_num)
			c.Lock.Unlock()
			replys.Is_success = true
			return nil
		}
	}
	c.Lock.Unlock()
	return nil
}
