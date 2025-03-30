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
	lock             *sync.Mutex
	curStage         Stage
	needProcessTasks []Task
	pendingTaskMap   map[Task]*time.Timer
	nReduceNum       int
	maxTaskId        int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) stageManage() {
	log.Printf("stage{%v}, pendingTask{%v} needProcessTask{%v}", c.curStage, len(c.pendingTaskMap), len(c.needProcessTasks))
	if c.curStage == MapperStage {
		if len(c.pendingTaskMap) == 0 && len(c.needProcessTasks) == 0 {
			log.Printf("stage change to Reduce")
			c.curStage = ReduceStage
			for i := 0; i < c.nReduceNum; i++ {
				c.needProcessTasks = append(c.needProcessTasks, Task{ReduceTaskId: i, TaskType: ReduceTaskType, MapTaskId: c.maxTaskId})
			}
		}
	} else if c.curStage == ReduceStage {
		if len(c.pendingTaskMap) == 0 && len(c.needProcessTasks) == 0 {
			log.Printf("stage change to Finish")
			c.curStage = FinishStage
		}
	}
}
func (c *Coordinator) pullTimeOutTaskToNeedProcessList() int {
	pullCnt := 0
	for task, timer := range c.pendingTaskMap {
		select {
		case <-timer.C:
			delete(c.pendingTaskMap, task)
			c.needProcessTasks = append(c.needProcessTasks, task)
			pullCnt++
			log.Printf("[warning] task{%v} timeout", task)
			// log
		default:
			// 未超时，继续执行其他操作
		}
	}
	return pullCnt
}

func (c *Coordinator) SyncAck(args *SyncAckArgs, reply *SyncAckReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, task := range args.Tasks {
		delete(c.pendingTaskMap, task)
	}
	c.stageManage()
	return nil
}
func (c *Coordinator) GetTasks(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	requestNum := args.TaskNum
	// if requestNum > len(c.needProcessTasks) {
	// 	// 返回特定错误
	// 	log.Printf("[warning] not task can process")
	// 	return http.ErrAbortHandler
	// }
	reply.Tasks = []Task{}
	if c.curStage == FinishStage {
		reply.Tasks = append(reply.Tasks, Task{TaskType: ExitTaskType})
		return nil
	}
	for i := 0; i < requestNum; i++ {
		if len(c.needProcessTasks) == 0 {
			if c.pullTimeOutTaskToNeedProcessList() == 0 {
				// 返回特定错误
				log.Printf("[warning] not task can process")
				return http.ErrAbortHandler
			}
		}
		task := c.needProcessTasks[len(c.needProcessTasks)-1]
		c.needProcessTasks = c.needProcessTasks[:len(c.needProcessTasks)-1]
		reply.Tasks = append(reply.Tasks, task)
		c.pendingTaskMap[task] = time.NewTimer(0)
		c.pendingTaskMap[task].Reset(4)
	}
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.curStage == FinishStage {
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.curStage = MapperStage
	c.lock = new(sync.Mutex)
	c.needProcessTasks = make([]Task, 0)
	c.maxTaskId = len(files)
	for i, fileName := range files {
		c.needProcessTasks = append(c.needProcessTasks, Task{MaprTaskType, fileName, i, -1, nReduce})
	}
	c.pendingTaskMap = make(map[Task]*time.Timer)
	c.nReduceNum = nReduce
	c.server()
	return &c
}
