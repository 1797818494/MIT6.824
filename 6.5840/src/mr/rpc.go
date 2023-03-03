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
	Task_type       string
	File_name       string
	Available_tasks chan Task
	Num_reduce      int
	Task_id         int
	Key             string
	Files_set       Key_and_files_set
	IsPush          bool
}

type RpcArgs struct {
	Type_request  string
	Task_finished bool
	Task_id       int
	Internal_file map[string][]string
}

type RpcReply struct {
	Task       Task
	Is_success bool
	Conflict   bool
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
