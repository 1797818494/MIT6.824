package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func CreateFile(reduce_id int) *os.File {
	filename := fmt.Sprintf("mr-out-%d", reduce_id)
	file_io, ok := os.Create(filename)
	if ok != nil {
		log.Fatal("cra")
	}
	return file_io
}

type key_file struct {
	Key   string
	files []string
}

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		args := RpcArgs{
			task_finished: false,
		}
		replys := RpcReply{}
		ok := call("Coordinator.ApplyForTask", &args, &replys)
		// the position may not be suitable
		if replys.conflict {
			log.Fatal("confilct exit")
			os.Exit(0)
		}
		if !ok {
			log.Fatal("Rpc call failed")
			os.Exit(0)
		}
		if !replys.is_success {

			os.Exit(0)
		}
		if replys.task.task_type == "Map" {
			log.Println("Start Map worker")
			intermediate := []KeyValue{}
			file, err := os.Open(replys.task.file_name)
			if err != nil {
				log.Fatalf("cannot open %v", replys.task.task_type)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", replys.task.file_name)
			}
			file.Close()
			kva := mapf(replys.task.file_name, string(content))
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))
			i := 0
			var key_and_file_name []key_file
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				var files []string
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				reduce_id := ihash(intermediate[i].Key) % replys.task.num_reduce
				file_name := fmt.Sprintf("mr-%d-%d", replys.task.task_id, reduce_id)
				file, ok := os.Create(file_name)
				// to do temp file
				if ok != nil {
					log.Fatalln("os Create fatal ", replys.task.task_id)
				}
				files = append(files, file_name)
				enc := json.NewEncoder(file)
				for _, value := range values {
					err := enc.Encode(&KeyValue{
						Key:   intermediate[i].Key,
						Value: value,
					})
					if err != nil {
						log.Fatalln("encode fail")
					}
				}
				append_item := key_file{
					Key:   intermediate[i].Key,
					files: files,
				}
				key_and_file_name = append(key_and_file_name, append_item)
				file.Close()
				i = j
			}
			internal_files := make(map[string][]string)
			for _, this_kv := range key_and_file_name {
				key := this_kv.Key
				files := this_kv.files
				internal_files[key] = append(internal_files[key], files...)
			}
			args := RpcArgs{
				type_request:  "Map",
				task_finished: true,
				internal_file: internal_files,
			}
			replys := RpcReply{}
			ok := call("Coordinator.ApplyForTask", &args, &replys)
			if !ok {
				log.Fatalln("rpc fail when map finish")
				os.Exit(0)
			}
			log.Println("map worker reply the master")
			continue

		}
		if replys.task.task_type == "Reduce" {
			log.Println("Start Reduce worker")
			var err error
			var file_io *os.File
			defer file_io.Close()
			for i, this_kv := range replys.task.key_files {
				var values []string
				Key := this_kv.Key
				file_names := this_kv.files
				for _, file := range file_names {
					file_io, err := os.Open(file)
					if err != nil {
						log.Fatalln("open fail")
					}
					dec := json.NewDecoder(file_io)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						if Key == kv.Key {
							values = append(values, kv.Value)
						}
					}
				}
				if i == 0 {
					reduce_id := ihash(Key) % replys.task.num_reduce
					file_name := fmt.Sprintf("mr-out%d", reduce_id)
					file_io, err = os.Create(file_name)
					if err != nil {
						log.Fatalln("create file fail")
					}
				}
				result := reducef(Key, values)
				enc := json.NewEncoder(file_io)
				result_kv := KeyValue{
					Key:   Key,
					Value: result,
				}
				enc.Encode(&result_kv)
			}
		}
		return_args := RpcArgs{
			type_request:  "Reduce",
			task_finished: true,
			task_id:       replys.task.task_id,
		}
		return_replys := RpcReply{}
		ok1 := call("Coordinator.ApplyForTask", &return_args, &return_replys)
		if !ok1 {
			log.Fatalln("rpc fail when Reduce finish")
			os.Exit(0)
		}
		log.Println("Reducer worker reply the master")
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
		continue
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
