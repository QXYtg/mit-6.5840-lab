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
type AskForTaskRequest struct {
}

type TaskType uint

const (
	MAP TaskType = iota + 1
	REDUCE
)

type AskForTaskResponse struct {
	/**
	是否请求到任务, true: 是；false: 否
	*/
	ExistTask bool
	/**
	任务类型
	*/
	TaskType TaskType
	/**
	任务号
	*/
	TaskNo string
	/**
	文件路径
	*/
	FilePaths []string
	/**
	reduce任务数量
	*/
	ReduceCount uint
}

type CompleteTaskRequest struct {
	/**
	任务号
	*/
	TaskNo string
	/**
	任务类型
	*/
	TaskType TaskType
	/**
	文件路径，如果是map类型的任务则列表中的下标为对应的reduce任务的序号
	*/
	ResultFilePaths []string
	/**
	异常
	*/
	Error error
}

type CompleteTaskResponse struct {
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
