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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 请求分配任务
type AssignTaskArgs struct {
}

type AssignTaskReply struct {
	Type     string
	WorkId   int
	FileName string
	AcceptId int32 // 回复时需要的唯一标识符
	NReduce  int
	NMap     int
}

// 处理完之后报告状态。
type ReportTaskArgs struct {
	Type     string
	WorkId   int
	AcceptId int32
	Status   string
}

type ReportTaskReply struct {
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
