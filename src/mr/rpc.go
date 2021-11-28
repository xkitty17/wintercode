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
type MapNumArgs struct {
}
type MapNumReply struct {
	Num int
}

type MapTaskArgs struct {
	Content string
}

type MapTaskReply struct {
	File     string
	MapTaskID int
}

type MapTaskDoneArgs struct {
	Content  string
	FileName string
}
type MapTaskDoneReply struct {
	Done bool
}
type ReduceArgs struct {
	Content string
}

type ReduceReply struct {
	ReduceTaskID int
}

type ReduceTaskDoneArgs struct {
	TaskID int
}
type ReduceTaskDoneReply struct {
}

type MROverArgs struct {
}

type MROverReply struct {
	Done bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
