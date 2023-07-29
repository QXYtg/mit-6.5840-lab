package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	configLogFile(fmt.Sprintf("./worker-%v-%v", time.Now().Format(time.DateTime), os.Getpid()))
	for {
		handleTask(mapf, reducef)
		time.Sleep(100 * time.Millisecond)
	}
}

func configLogFile(logPath string) {
	// log to custom file
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	// Set log out put and enjoy :)
	log.SetOutput(logFile)

	// optional: log date-time, filename, and line number
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

/*
*
处理任务，存在任务并处理成功返回true,否则false
*/
func handleTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := AskForTaskRequest{}
	reply := AskForTaskResponse{}

	log.Printf("[handleTask#%v]请求任务...\n", os.Getpid())
	ok := call("Coordinator.AskForTask", &args, &reply)
	if !ok {
		log.Printf("[handleTask#%v]请求失败\n", os.Getpid())
		return
	}
	if !reply.ExistTask {
		log.Printf("[handleTask#%v]未请求到任务\n", os.Getpid())
		return
	}

	if reply.TaskType == MAP {
		completeTask(handleMapTask(&reply, mapf))
	} else {
		completeTask(handleReduceTask(&reply, reducef))
	}
	return
}

func handleReduceTask(reply *AskForTaskResponse, reducef func(string, []string) string) CompleteTaskRequest {
	log.Println("[handleReduceTask]处理开始，reply：", reply)
	intermediate := []KeyValue{}
	// 反序列化所有文件
	for _, filename := range reply.FilePaths {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("[handleReduceTask]无法打开文件： %v", filename)
			return CompleteTaskRequest{Error: errors.New("[handleMapTask]请求文件数量错误")}
		}

		// 反序列化
		kvs := []KeyValue{}
		dec := json.NewDecoder(file)
		err = dec.Decode(&kvs)
		if err != nil {
			log.Printf("[handleReduceTask]反序列化失败： %v", err)
			return CompleteTaskRequest{Error: err}
		}
		intermediate = append(intermediate, kvs...)
	}
	// 排序
	sort.Sort(ByKey(intermediate))

	// 创建临时文件
	tFile, err := os.CreateTemp("./", "temp-*.txt")
	if err != nil {
		log.Println("[handleReduceTask]创建临时文件出错", err)
		return CompleteTaskRequest{Error: err}
	}
	defer os.Remove(tFile.Name())

	// 写入临时文件
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	// 改名操作
	oname := fmt.Sprintf("mr-out-%v", reply.TaskNo)
	err = os.Rename(tFile.Name(), oname)
	if err != nil {
		log.Println("[handleReduceTask]改名操作失败", err)
		return CompleteTaskRequest{Error: err}
	}
	return CompleteTaskRequest{reply.TaskNo, reply.TaskType, []string{oname}, nil}
}

func handleMapTask(reply *AskForTaskResponse, mapf func(string, string) []KeyValue) CompleteTaskRequest {
	log.Println("[handleMapTask]处理开始，reply：", reply)
	intermediate := []KeyValue{}
	var intermediateFilePath []string
	// 正常一次map任务只有一个文件
	if len(reply.FilePaths) != 1 {
		log.Println("[handleMapTask]请求文件数量错误")
		return CompleteTaskRequest{Error: errors.New("[handleMapTask]请求文件数量错误")}
	}
	filename := reply.FilePaths[0]
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Printf("[handleMapTask]无法打开文件 %v, error %v\n", filename, err)
		return CompleteTaskRequest{Error: err}
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("[handleMapTask]无法读取文件 %v, error %v\n", filename, err)
		return CompleteTaskRequest{Error: err}
	}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// 将输出按reduce任务数量分组
	intermediateFileBucket := make([][]KeyValue, reply.ReduceCount)
	for _, kv := range intermediate {
		h := ihash(kv.Key) % int(reply.ReduceCount)
		intermediateFileBucket[h] = append(intermediateFileBucket[h], kv)
	}
	// 序列化保存中间文件
	for i, b := range intermediateFileBucket {
		// 创建临时文件
		tFile, err := os.CreateTemp("./", "temp-*.txt")
		defer os.Remove(tFile.Name())
		if err != nil {
			log.Println("[handleMapTask]创建临时文件出错", err)
			return CompleteTaskRequest{Error: err}
		}

		// 写入临时文件，整个bucket一起序列化写入
		enc := json.NewEncoder(tFile)
		err = enc.Encode(&b)
		if err != nil {
			log.Println("[handleMapTask]写入临时文件失败", err)
			return CompleteTaskRequest{Error: err}
		}

		// 改名操作
		oname := fmt.Sprintf("mr-%v-%v", reply.TaskNo, i)
		err = os.Rename(tFile.Name(), oname)
		if err != nil {
			log.Println("[handleMapTask]改名操作失败", err)
			return CompleteTaskRequest{Error: err}
		}
		intermediateFilePath = append(intermediateFilePath, oname)
	}
	return CompleteTaskRequest{reply.TaskNo, reply.TaskType, intermediateFilePath, nil}
}

func completeTask(request CompleteTaskRequest) {
	log.Println("[completeTask]任务处理结束回调开始，request：", request)
	reply := CompleteTaskResponse{}

	ok := call("Coordinator.CompleteTask", &request, &reply)
	if !ok {
		log.Println("[completeTask]任务处理结束回调失败")
		return
	}
	log.Println("[completeTask]任务处理结束回调成功")
	return
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
