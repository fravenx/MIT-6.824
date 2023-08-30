package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AskTaskArgs struct {
}

type AskTaskReply struct {
	Task       string
	SartReduce bool
}

type AskReduceNumArgs struct {
}

type AskReduceNumReply struct {
	ReduceNum int
}

type AskReduceArgs struct {
}

type AskReduceReply struct {
	ReduceNum int
}

type MapSuccessArgs struct {
	Task string
}

type MapSuccessReply struct {
}

type ReduceSuccessArgs struct {
	ReduceNum int
}

type ReduceSuccessReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
