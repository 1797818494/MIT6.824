package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type Task struct {
	task_type       string
	file_name       string
	available_tasks chan Task
	num_reduce      int
	task_id         int
	key             string
	file_names      []string
	key_files       []key_file
}

type RpcArgs struct {
	type_request  string
	task_finished bool
	task_id       int
	internal_file map[string][]string
}

type RpcReply struct {
	task       Task
	is_success bool
	conflict   bool
}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
