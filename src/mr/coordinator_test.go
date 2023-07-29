package mr

import (
	"fmt"
	"testing"
	"time"
)

var files = []string{"pg-being_ernest.txt", "pg-dorian_gray.txt"}

func TestMakeCoordinator(t *testing.T) {
	MakeCoordinator(files, 10)
}

func TestCoordinator_normalGetMapTaskAndComplete(t *testing.T) {
	TestMakeCoordinator(nil)
	c := Coordinator{}
	askForTaskResponse := AskForTaskResponse{}
	c.AskForTask(&AskForTaskRequest{}, &askForTaskResponse)
	if !askForTaskResponse.ExistTask {
		t.Fatalf(`No task!`)
	}

	resultFilePath := make([]string, 10)
	for i := 0; i < 10; i++ {
		resultFilePath[i] = fmt.Sprintf("mr-%v-%v", askForTaskResponse.TaskNo, i)
	}
	completeTaskRequest := CompleteTaskRequest{askForTaskResponse.TaskNo, askForTaskResponse.TaskType, resultFilePath, nil}
	c.CompleteTask(&completeTaskRequest, &CompleteTaskResponse{})
}

func TestCoordinator_normalGetMapTaskAndNoTaskFound(t *testing.T) {
	// 所有任务都RUNNING所以GET不到的情况
	TestMakeCoordinator(nil)
	c := Coordinator{}
	askForTaskResponse := AskForTaskResponse{}
	for i := 0; i < len(files); i++ {
		getTaskOnce(&askForTaskResponse, &c)
	}
	getTaskOnce(&askForTaskResponse, &c)
	if askForTaskResponse.ExistTask {
		t.Fatalf(`exist task!`)
	}
}

func getTaskOnce(askForTaskResponse *AskForTaskResponse, c *Coordinator) {
	c.AskForTask(&AskForTaskRequest{}, askForTaskResponse)
}

func completeTaskOnce(askForTaskResponse *AskForTaskResponse, c *Coordinator) {
	resultFilePath := make([]string, 10)
	for i := 0; i < 10; i++ {
		resultFilePath[i] = fmt.Sprintf("mr-%v-%v", askForTaskResponse.TaskNo, i)
	}
	completeTaskRequest := CompleteTaskRequest{askForTaskResponse.TaskNo, askForTaskResponse.TaskType, resultFilePath, nil}
	c.CompleteTask(&completeTaskRequest, &CompleteTaskResponse{})
}

func TestCoordinator_normalGetAndTimeOutAndThenGet(t *testing.T) {
	// 任务超时后再get能get到
	TestMakeCoordinator(nil)
	c := Coordinator{}
	askForTaskResponse := AskForTaskResponse{}
	for i := 0; i < len(files); i++ {
		getTaskOnce(&askForTaskResponse, &c)
	}
	time.Sleep(time.Duration(WAIT_TIME+1) * time.Second)
	getTaskOnce(&askForTaskResponse, &c)
	if !askForTaskResponse.ExistTask {
		t.Fatalf(`not exist task!`)
	}
}

func TestCoordinator_normalGetReduceTask(t *testing.T) {
	// 所有任务都RUNNING所以GET不到的情况
	TestMakeCoordinator(nil)
	c := Coordinator{}
	for i := 0; i < len(files); i++ {
		askForTaskResponse := AskForTaskResponse{}
		getTaskOnce(&askForTaskResponse, &c)
		completeTaskOnce(&askForTaskResponse, &c)
	}
	for i := 0; i < 10; i++ {
		askForTaskResponse := AskForTaskResponse{}
		getTaskOnce(&askForTaskResponse, &c)
		completeTaskOnce(&askForTaskResponse, &c)
		if !askForTaskResponse.ExistTask || askForTaskResponse.TaskType != REDUCE {
			t.Fatalf(`not exist reduceTask`)
		}
	}
	askForTaskResponse := AskForTaskResponse{}
	getTaskOnce(&askForTaskResponse, &c)
	if askForTaskResponse.ExistTask || askForTaskResponse.TaskType != REDUCE {
		t.Fatalf(`get task!!`)
	}
}
