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
	"time"
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

type Key_file struct {
	Key   string
	Files []string
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
	log.Println("worker start")
	// Your worker implementation here.
	for {
		args := RpcArgs{
			Task_finished: false,
		}
		replys := RpcReply{}
		ok := call("Coordinator.ApplyForTask", &args, &replys)
		// the position may not be suitable
		if replys.Conflict {
			log.Println("confilct continue")
			continue
		}
		if !ok {
			log.Fatal("Rpc call failed")
			os.Exit(0)
		}
		if !replys.Is_success {
			log.Println("reply is not success")
			os.Exit(0)
		}
		if replys.Task.Task_type == "Map" {
			log.Println("Start Map worker")
			intermediate := []KeyValue{}
			file, err := os.Open(replys.Task.File_name)
			log.Println(replys.Task.File_name)
			if err != nil {
				log.Fatalf("cannot open %v\n", replys.Task.Task_type)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", replys.Task.File_name)
				os.Exit(0)
			}
			file.Close()
			kva := mapf(replys.Task.File_name, string(content))
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))
			i := 0
			var key_and_file_name []Key_file
			log.Println("i max is", len(intermediate))
			reduce_file_map := make(map[int]bool)
			reduce_json := make(map[int]*json.Encoder)
			reduce_file := make(map[int]*os.File)
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
				reduce_id := ihash(intermediate[i].Key) % replys.Task.Num_reduce
				file_name := fmt.Sprintf("mr-%d-%d", replys.Task.Task_id, reduce_id)
				var file *os.File
				var ok error
				if !reduce_file_map[reduce_id] {
					log.Println("reduce_id ", reduce_id)
					reduce_file_map[reduce_id] = true
					// file, ok = os.Create(file_name)
					file, ok = ioutil.TempFile(".", "*")
					reduce_file[reduce_id] = file
					// to do temp file
					if ok != nil {
						log.Fatalln("os Create fatal ", replys.Task.Task_id, err)
					}
					reduce_json[reduce_id] = json.NewEncoder(file)
				}
				files = append(files, file_name)
				//log.Printf("create mr-%d-%d", replys.Task.Task_id, reduce_id)
				enc := reduce_json[reduce_id]
				for _, value := range values {
					err := enc.Encode(&KeyValue{
						Key:   intermediate[i].Key,
						Value: value,
					})
					if err != nil {
						log.Fatalln("encode fail", err)
					}
				}
				append_item := Key_file{
					Key:   intermediate[i].Key,
					Files: files,
				}
				key_and_file_name = append(key_and_file_name, append_item)
				i = j
			}
			for reduce_this_id, is_true := range reduce_file_map {
				if is_true {
					reduce_file[reduce_this_id].Close()
				}
			}
			internal_files := make(map[string][]string)
			for _, this_kv := range key_and_file_name {
				key := this_kv.Key
				files := this_kv.Files
				internal_files[key] = append(internal_files[key], files...)
			}
			for my_reduce_id, file_io := range reduce_file {
				s := fmt.Sprintf("./mr-%d-%d", replys.Task.Task_id, my_reduce_id)
				log.Println(s)
				os.Rename(file_io.Name(), s)
			}
			args := RpcArgs{
				Type_request:  "Map",
				Task_finished: true,
				Task_id:       replys.Task.Task_id,
				Internal_file: internal_files,
			}
			// rename the file
			replys := RpcReply{}
			ok := call("Coordinator.ApplyForTask", &args, &replys)
			if !ok {
				log.Fatalln("rpc fail when map finish")
				os.Exit(0)
			}
			log.Println("map worker reply the master")
			time.Sleep(800 * time.Millisecond)
			continue

		}
		if replys.Task.Task_type == "Reduce" {
			log.Println("Start Reduce worker")
			var err error
			var file_io *os.File
			defer file_io.Close()
			reduce_map := replys.Task.Files_set
			Key_Value := make(map[string][]string)
			reduce_id := replys.Task.Task_id
			// file_name := fmt.Sprintf("mr-out-%d", reduce_id)
			file_io, err = ioutil.TempFile(".", "*")
			// file_io, err = os.Create(file_name)
			if err != nil {
				log.Fatalln("create file fail")
			}
			log.Println("reduce mr-out succeed", reduce_id)
			for file, _ := range reduce_map.Files {
				//log.Println(file)
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
					// log.Println("file key is", kv.Key)
					if reduce_map.Key[kv.Key] {
						Key_Value[kv.Key] = append(Key_Value[kv.Key], kv.Value)
					}
				}
			}
			for Key, Value := range Key_Value {
				//log.Println(Key, Value)
				log.Println(reducef("c", []string{"1", "1", "1", "1", "1"}))
				result := reducef(Key, Value)
				log.Println(Key, result)
				fmt.Fprintf(file_io, "%v %v\n", Key, result)
			}
			// log.Println(Key, "+", values)
			// enc := json.NewEncoder(file_io)
			// result_kv := KeyValue{
			// 	Key:   Key,
			// 	Value: result,
			// }
			// enc.Encode(&result_kv)
			log.Println("the reduce rename !!!!  ", reduce_id)
			os.Rename(file_io.Name(), fmt.Sprintf("./mr-out-%d", reduce_id))
			return_args := RpcArgs{
				Type_request:  "Reduce",
				Task_finished: true,
				Task_id:       replys.Task.Task_id,
			}
			log.Println("reduce finish")
			return_replys := RpcReply{}
			ok1 := call("Coordinator.ApplyForTask", &return_args, &return_replys)
			if !ok1 {
				log.Fatalln("rpc fail when Reduce finish")
				os.Exit(0)
			}
			log.Println("Reducer worker reply the master")
			// uncomment to send the Example RPC to the coordinator.
			// CallExample()
			time.Sleep(800 * time.Millisecond)
			continue
		}
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
