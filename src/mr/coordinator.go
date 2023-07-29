package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

const WAIT_TIME int64 = 10

/*
*
rpc接口1：worker请求任务
*/
func (c *Coordinator) AskForTask(request *AskForTaskRequest, response *AskForTaskResponse) error {
	log.Println("[AskForTask]开始")
	response.ExistTask = false
	idleMapTask := MapTasks.getTaskAndChangeStatusSafe(IDLE, RUNNING)
	if idleMapTask != nil {
		log.Println("[AskForTask]开始分配map task，task：", idleMapTask)
		// 创建定时器并关联
		go time.AfterFunc(time.Duration(WAIT_TIME)*time.Second, func() {
			log.Println("[AskForTask.timer]定时器超时，taskNo：", idleMapTask.taskNo)
			task := MapTasks.changeStatusByTaskNoSafe(idleMapTask.taskNo, RUNNING, IDLE)
			if task == nil {
				log.Println("[AskForTask.timer]任务已完成，taskNo：", idleMapTask.taskNo)
			} else {
				log.Println("[AskForTask.timer]重置任务状态为IDLE，taskNo：", idleMapTask.taskNo)
			}
		})
		// 填充返回值
		response.ExistTask = true
		response.TaskType = MAP
		response.TaskNo = idleMapTask.taskNo
		response.FilePaths = idleMapTask.filePath
		response.ReduceCount = MapTasks.nReduce
	} else if len(MapTasks.status2Tasks[RUNNING]) != 0 {
		// 不可分配map task但还存在运行中的map task
		log.Println("[AskForTask]等待所有map task结束执行...")
	} else {
		// 所有map任务都完成，开始分配reduce任务
		// 用一种trick的方法判断是否初始化了
		initReduceTasks()
		targetReduceTask := ReduceTasks.getTaskAndChangeStatusSafe(IDLE, RUNNING)
		// 无运行态的任务，看是否有任务处在备份调度的状态
		if targetReduceTask == nil {
			targetReduceTask = ReduceTasks.getTaskAndChangeStatusSafe(BACKUP_SCHEDULING, RUNNING)
		}
		if targetReduceTask == nil {
			log.Println("[AskForTask]等待所有reduce task结束执行...")
		} else {
			log.Println("[AskForTask]开始分配reduce task，task：", targetReduceTask)

			// 创建定时器并关联
			go time.AfterFunc(time.Duration(WAIT_TIME)*time.Second, func() {
				task := ReduceTasks.changeStatusByTaskNoSafe(targetReduceTask.taskNo, RUNNING, IDLE)
				if task == nil {
					log.Println("[AskForTask.timer]任务已完成，taskNo：", targetReduceTask.taskNo)
				} else {
					log.Println("[AskForTask.timer]重置任务状态为IDLE，taskNo：", targetReduceTask.taskNo)
				}
			})
			// 填充返回值
			response.ExistTask = true
			response.TaskType = REDUCE
			response.TaskNo = targetReduceTask.taskNo
			response.FilePaths = targetReduceTask.filePath
			response.ReduceCount = MapTasks.nReduce
		}
	}
	log.Println("[AskForTask]分配任务结束，response：", response)
	return nil
}

/*
*
rpc接口2：worker完成任务回调
*/
func (c *Coordinator) CompleteTask(request *CompleteTaskRequest, response *CompleteTaskResponse) error {
	log.Println("[CompleteTask]completeTaskRequest", request)
	if request.Error != nil {
		log.Println("[CompleteTask]worker内部处理异常", request.Error)
		return nil
	}
	if request.TaskType == MAP {
		// 任务状态非RUNNING，不处理
		log.Println("[CompleteTask]更改map task状态为已完成，taskNo：", request.TaskNo)
		task := MapTasks.changeStatusByTaskNoSafe(request.TaskNo, RUNNING, COMPLETED)
		if task == nil {
			log.Println("[CompleteTask]任务状态非RUNNING，不处理，taskNo：", request.TaskNo)
			return nil
		}
		// 按reduce序号分组文件路径
		for i, f := range request.ResultFilePaths {
			intermediateFiles[i] = append(intermediateFiles[i], f)
		}
	} else if request.TaskType == REDUCE {
		log.Println("[CompleteTask]更改reduce task状态为已完成，taskNo：", request.TaskNo)
		ReduceTasks.changeStatusByTaskNoSafe(request.TaskNo, RUNNING, COMPLETED)
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
	// 完成的任务数量 == reduce任务数量
	if int(ReduceTasks.nReduce) != 0 && len(ReduceTasks.status2Tasks[COMPLETED]) == int(ReduceTasks.nReduce) {
		log.Printf("[Done]mapreduce结束，ReduceTasks: %v\n", ReduceTasks)
		return true
	}
	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	configLogFile(fmt.Sprintf("./coordinator-%v-%v", time.Now().Format(time.DateTime), os.Getpid()))
	log.Println("[MakeCoordinator]开始files：", files, "nReduce：", nReduce)
	// 初始化map task和一些中间变量
	initMapTasks(files, nReduce)
	// 完成初始化map task后开始监听rpc请求
	c := Coordinator{}
	c.server()
	return &c
}

type Tasks struct {
	nReduce      uint
	tasks        []*Task
	status2Tasks map[TaskStatus][]*Task
	mu           sync.Mutex
}

type Task struct {
	taskNo   string
	taskType TaskType
	status   TaskStatus
	filePath []string
}

/*
*
构造1.任务状态到任务列表的映射 2.任务号到任务的映射
*/
func (ts *Tasks) buildTasksMap() {
	if ts.tasks == nil {
		return
	}
	ts.status2Tasks = make(map[TaskStatus][]*Task)
	//for i := IDLE; i <= BACKUP_SCHEDULING; i++ {
	//	ts.status2Tasks[i] = make([]*Task, 0)
	//}
	for _, tp := range ts.tasks {
		ts.status2Tasks[tp.status] = append(ts.status2Tasks[tp.status], tp)
	}

}

/*
*
获取对应状态的任务，获取不到返回空。线程安全的
*/
func (ts *Tasks) getTaskAndChangeStatusSafe(fromStatus TaskStatus, toStatus TaskStatus) *Task {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(ts.status2Tasks[fromStatus]) > 0 {
		fromSlice := ts.status2Tasks[fromStatus]
		first := fromSlice[0]
		ts.status2Tasks[fromStatus] = fromSlice[1:]
		ts.status2Tasks[toStatus] = append(ts.status2Tasks[toStatus], first)
		first.status = toStatus
		return first
	}
	return nil
}

/*
*
根据任务号和任务起始状态更新任务状态为一个新值，线程安全
*/
func (ts *Tasks) changeStatusByTaskNoSafe(taskNo string, fromStatus TaskStatus, toStatus TaskStatus) *Task {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	fromSlice := ts.status2Tasks[fromStatus]
	for i, task := range fromSlice {
		if task.taskNo == taskNo {
			// 将任务从一个状态的列表放到另一个状态的列表
			ts.status2Tasks[fromStatus] = append(fromSlice[:i], fromSlice[i+1:]...)
			ts.status2Tasks[toStatus] = append(ts.status2Tasks[toStatus], task)
			task.status = toStatus
			return task
		}
	}
	return nil
}

type TaskStatus uint

const (
	IDLE TaskStatus = iota + 1
	RUNNING
	COMPLETED
	BACKUP_SCHEDULING
)

var MapTasks Tasks
var intermediateFiles [][]string

func initMapTasks(files []string, nReduce int) {
	log.Println("[initMapTasks]初始化MapTasks...")
	// 构造任务列表
	tasks := make([]*Task, len(files))
	for i, filename := range files {
		tasks[i] = &Task{taskNo: fmt.Sprintf("%s%d", "MAPTASK_", i), taskType: MAP, status: IDLE, filePath: []string{filename}}
	}
	// 初始化map任务的聚合结构
	MapTasks = Tasks{nReduce: uint(nReduce), tasks: tasks}
	// 初始化中间文件列表
	intermediateFiles = make([][]string, nReduce)
	// 构造状态->任务列表的映射
	MapTasks.buildTasksMap()
	log.Println("[initMapTasks]MapTasks初始化完成：", MapTasks)
}

var ReduceTasks Tasks

func initReduceTasks() {
	if len(ReduceTasks.tasks) != 0 {
		return
	}
	log.Println("[initReduceTasks]初始化ReduceTasks...")
	nReduce := len(intermediateFiles)
	// 构造任务列表
	tasks := make([]*Task, nReduce)
	for i, fileNames := range intermediateFiles {
		tasks[i] = &Task{taskNo: fmt.Sprintf("%s%d", "REDUCETASK_", i), taskType: REDUCE, status: IDLE, filePath: fileNames}
	}
	// 初始化map任务的字段
	ReduceTasks.mu.Lock()
	if len(ReduceTasks.tasks) == 0 { // 确保只初始化一次
		ReduceTasks.nReduce = uint(nReduce)
		ReduceTasks.tasks = tasks
	}
	// 构造状态->任务列表的映射
	ReduceTasks.buildTasksMap()
	ReduceTasks.mu.Unlock()
	log.Println("[initReduceTasks]ReduceTasks初始化完成：", ReduceTasks)
}
