package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"

	//"sort"
	"time"
	// "golang.org/x/sys/unix"
)

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// 定义任务 ID 结构
type TaskIdentifier struct {
	MapTaskId int
	ReduceId  int
	FilePath  string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//workerId := unix.Gettid()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := RequestTaskReply{}
		args := RequestTaskArgs{1}
		ok := call("Coordinator.GetTasks", &args, &reply)
		if ok {
			for _, task := range reply.Tasks {
				doProcessTask(task, mapf, reducef)
			}
		} else {
			log.Printf("not ok")
		}
		ackArgs := SyncAckArgs{reply.Tasks}
		ackReply := SyncAckReply{}
		ackOk := call("Coordinator.SyncAck", &ackArgs, &ackReply)
		if ackOk {
			log.Printf("%v ack ok, ack args = {%v}", 0, ackArgs)
		} else {
			log.Printf("not ok")
		}
		time.Sleep(800 * time.Millisecond)
	}

}

func doProcessTask(task Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Printf("task{%v} start executor", task)
	defer log.Printf("task{%v} finish", task)
	if task.TaskType == ExitTaskType {
		os.Exit(0)
	}
	if task.TaskType == MaprTaskType {
		fileName := task.FileName
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("can not open ", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal("cannot read ", fileName)
		}
		file.Close()
		kva := mapf(fileName, string(content))
		reduceVec := make(map[int][]KeyValue)
		for _, kv := range kva {
			index := ihash(kv.Key) % task.ReduceNum
			reduceVec[index] = append(reduceVec[index], kv)
		}
		for reduceId, kvs := range reduceVec {
			tmpfile, ok := ioutil.TempFile(".", "*")
			if ok != nil {
				log.Fatal("os create tempFile fail")
			}
			encoder := json.NewEncoder(tmpfile)
			//sort.Sort(ByKey(kvs))
			for _, kv := range kvs {
				err := encoder.Encode(kv)
				if err != nil {
					log.Fatal("encoder fail")
				}
			}
			s := fmt.Sprintf("./mr-%d-%d", task.MapTaskId, reduceId)
			os.Rename(tmpfile.Name(), s)
		}
	}
	if task.TaskType == ReduceTaskType {
		reduceId := task.ReduceTaskId
		keyValues := make(map[string][]string)
		mapperMaxId := 10
		for i := 0; i < mapperMaxId; i++ {
			fileName := fmt.Sprintf("./mr-%d-%d", i, reduceId)
			file, err := os.Open(fileName)
			if err != nil {
				log.Printf("[warning] fileName not exist, filename ={%v}", fileName)
				continue
			}
			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				// log.Println("file key is", kv.Key)

				keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
			}
		}
		tmpFile, err := ioutil.TempFile(".", "*")
		if err != nil {
			log.Fatal("tmpFile create error")
		}
		for Key, Values := range keyValues {
			//log.Println(Key, Value)
			log.Println(reducef("c", []string{"1", "1", "1", "1", "1"}))
			result := reducef(Key, Values)
			log.Println(Key, result)
			fmt.Fprintf(tmpFile, "%v %v\n", Key, result)
		}
		os.Rename(tmpFile.Name(), fmt.Sprintf("./mr-out-%d", reduceId))
	}

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
