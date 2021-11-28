package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

	// map 阶段
	mapTaskNum := getMapNum()
	for {
		mapFile, mapTaskID := getMapTask()
		if mapFile != "" {
			file, err := os.Open(mapFile)
			if err != nil {
				log.Fatalf("cannot open %v", mapFile)
			}
			content, err := ioutil.ReadAll(file)
			// 读取文件内容
			if err != nil {
				log.Fatalf("cannot read %v", mapFile)
			}
			file.Close()
			// 调用map函数
			kva := mapf(mapFile, string(content))
			// 将map的结果写入临时文件
			shuffle(mapTaskID, kva)
			// 通知调度器这个map task完成了
			getMapTaskDone("single", mapFile)

		} else {
			if getMapTaskDone("all", "") {
				break
			}
			time.Sleep(time.Second)
		}
	}

	// reduce 阶段
	for {
		reduceTaskID := getReduceTask()
		if reduceTaskID != -1 {
			kva := make([]KeyValue, 0)
			for j := 0; j < mapTaskNum; j++ {
				// 该reduce任务对应的中间文件名
				fileName := fmt.Sprintf("mr-%d-%d", j, reduceTaskID)
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				// 读取文件内容
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					// 保存结果
					kva = append(kva, kv)
				}
			}
			// reduce 操作
			storeReduceRes(reduceTaskID, kva, reducef)
			// 通知调度器一个reduce任务已完成
			getReduceTaskDone(reduceTaskID)
			// 判断是否全部任务已完成
		} else {
			if getMROver() {
				break
			}
		}
	}
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}


//
//获取map任务数量
//
func getMapNum() int {
	args := MapNumArgs{}
	reply := MapNumReply{}
	call("Master.MapTaskNum", &args, &reply)
	return reply.Num

}

//
// 获取新的map任务
//
func getMapTask() (string, int) {
	args := MapTaskArgs{}
	reply := MapTaskReply{}
	call("Master.MapTask", &args, &reply)
	return reply.File, reply.MapTaskID
}

//
// 通知 coordinator map任务已经完成
//
// content:
// `single`: 通知单个任务已经完成 \ `all`: 询问全部任务是否已经完成
//
func getMapTaskDone(content, fileName string) bool {

	args := MapTaskDoneArgs{Content: content, FileName: fileName}
	reply := MapTaskDoneReply{}
	call("Master.MapTaskDone", &args, &reply)
	return reply.Done
}

//
// 将map的结果保存至中间文件
//
func shuffle(mapTaskID int, intermediate []KeyValue) {
	interFiles := make([]*os.File, 10)
	for i := 0; i < 10; i++ {
		interFileName := fmt.Sprintf("mr-%d-%d", mapTaskID, i)
		interFiles[i], _ = os.Create(interFileName)
		defer interFiles[i].Close()
	}
	for _, kv := range intermediate {
		reduceTaskID := ihash(kv.Key) % 10
		// 确定对应的reduce任务
		enc := json.NewEncoder(interFiles[reduceTaskID])
		enc.Encode(&kv)
	}
}

//
// 获取reduce
//
func getReduceTask() int {
	args := ReduceArgs{}
	reply := ReduceReply{}
	call("Master.ReduceTask", &args, &reply)
	return reply.ReduceTaskID
}

//
// 将reduce任务执行结果保存在最终的文件中
//
func storeReduceRes(reduceTaskID int, intermediate []KeyValue, reducef func(string, []string) string) {
	fileName := fmt.Sprintf("mr-out-%d", reduceTaskID)
	ofile, _ := os.Create(fileName)
	defer ofile.Close()
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// reduce 操作
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}

//
// 通知某个reduce任务完成
//
func getReduceTaskDone(TaskID int) {
	args := ReduceTaskDoneArgs{TaskID: TaskID}
	reply := ReduceTaskDoneReply{}
	call("Master.ReduceTaskDone", &args, &reply)
}

//
// 询问是否全部map-reduce任务完成
//
func getMROver() bool {
	args := MROverArgs{}
	reply := MROverReply{}
	call("Master.MROver", &args, &reply)
	return reply.Done
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)

	return err == nil
}
