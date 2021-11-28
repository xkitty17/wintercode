package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	UnFinished = iota
	Finished
)


type Master struct {
	// Your definitions here.

	Files chan string // 文件名

	MapNum int // map task number
	MapTasksStatus map[string]int // 输入文件的状态
	MapTasks chan int // map tasks
	MapStatusLock sync.Mutex

	/*
	读写锁是针对读写的互斥锁
	基本遵循两大原则：
	1、可以随便读，多个goroutine同时读
	2、写的时候，啥也不能干。不能读也不能写
	*/

	ReduceNum int // reduce task number
	ReduceTasksStatus map[int]int // reduce task的状态
	ReduceTasks chan int // reduce tasks
	ReduceStatusLock sync.Mutex

	Over bool // 全部结束
	OverLock sync.Mutex
}



// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (m *Master) MapTaskNum(args *MapNumArgs, reply *MapNumReply) error {
	reply.Num = m.MapNum
	return nil
}

//创建一个 map task，rpc调用后，reply中拿到file和taskid
func (m *Master) MapTask(args *MapTaskArgs, reply *MapTaskReply) error{
	select {
	case reply.File = <- m.Files:
		reply.MapTaskID = <- m.MapTasks

		// 如果10s后拿走的任务还没有完成，重新放回tasks的chan中
		go func(taskFile string, taskId int) {
			time.Sleep(time.Second * 10)
			m.MapStatusLock.Lock()
			if m.MapTasksStatus[taskFile] == UnFinished{
				m.MapTasks <- taskId
				m.Files <- taskFile
			}
			m.MapStatusLock.Unlock()
		}(reply.File, reply.MapTaskID) // 函数实参

	default:
		reply.File = ""
		reply.MapTaskID = 0
	}

	return nil
}

func (m * Master) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error{
	// 通知单个任务结束
	if args.Content == "single"{
		m.MapStatusLock.Lock()
		m.MapTasksStatus[args.FileName] = Finished
		m.MapStatusLock.Unlock()
	} else if args.Content == "all"{
		// 查询所有任务是否结束
		reply.Done = true
		m.MapStatusLock.Lock()
		for _, v := range m.MapTasksStatus{
			if v == UnFinished{
				reply.Done = false
				break
			}
		}
		m.MapStatusLock.Unlock()
	}
	return nil
}

func (m *Master) ReduceTask(args *ReduceArgs, reply *ReduceReply) error{
	select {
	case reply.ReduceTaskID = <- m.ReduceTasks:
		go func(taskId int) {
			time.Sleep(time.Second * 10)
			m.ReduceStatusLock.Lock()
			if m.ReduceTasksStatus[taskId] == UnFinished{
				m.ReduceTasks <- taskId
			}
			m.ReduceStatusLock.Unlock()
		}(reply.ReduceTaskID) // 函数实参

	default:
		reply.ReduceTaskID = -1
	}
	return nil
}

func (m *Master) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error{
	m.ReduceStatusLock.Lock()
	m.ReduceTasksStatus[args.TaskID] = Finished
	m.ReduceStatusLock.Unlock()
	return nil
}

func (m *Master) MROver(args *MROverArgs, reply *MROverReply) error {
	m.OverLock.Lock()
	reply.Done = m.Over
	m.OverLock.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	sign := true
	m.ReduceStatusLock.Lock()
	for _, v := range m.ReduceTasksStatus{
		if v == UnFinished{
			sign = false
			//break TODO
		}
	}
	m.ReduceStatusLock.Unlock()

	m.OverLock.Lock()
	m.Over = sign
	m.OverLock.Unlock()

	return m.Over
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.

	m := Master{}
	m.Files = make(chan string, len(files))

	m.MapTasks = make(chan int, len(files))
	m.MapNum = len(files)
	m.MapTasksStatus = make(map[string]int)
	for idx, file := range files{
		m.Files <- file
		m.MapTasks <- idx
		m.MapTasksStatus[file] = UnFinished
	}

	m.ReduceNum = nReduce
	m.ReduceTasks = make(chan int, nReduce)
	m.ReduceTasksStatus = make(map[int]int)
	for i := 0; i < nReduce; i++{
		m.ReduceTasks <- i
		m.ReduceTasksStatus[i] = UnFinished
	}

	m.server()
	return &m
}
