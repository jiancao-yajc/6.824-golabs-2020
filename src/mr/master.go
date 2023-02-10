package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TaskMap = iota
	TaskReduce
	TaskWait
	TaskEnd
)

const (
	NOTASSIGNED = iota
	ASSIGNED
	FINISHED
)

type MapReduceTask struct {
	Index        int
	Type         int
	State        int
	Filename     []string
	TimeStamp    time.Time
	ReduceNumber int
}
type Master struct {
	// Your definitions here.

	MapList    []MapReduceTask
	ReduceList []MapReduceTask

	MapDone    int
	ReduceDone int
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Handler(args *Taskargs, reply *TaskReply) error {
	fmt.Println(len(m.MapList))
	m.mu.Lock()
	fmt.Println("status", m.MapDone, m.ReduceDone)
	var t MapReduceTask
	if m.MapDone == 0 && m.ReduceDone == 0 {
		t.Type = TaskEnd
		reply.Task = t
	} else if args.MessageType == RequestTask {
		if m.MapDone != 0 {
			flag := false
			for _, tmp := range m.MapList {
				if tmp.State == NOTASSIGNED {
					reply.Task = tmp
					tmp.State = ASSIGNED
					tmp.Type = TaskMap
					flag = true
					break
				}
			}
			if flag == false {
				t.Type = TaskWait
				reply.Task = t
			}
		} else if m.ReduceDone != 0 {
			flag := false
			for _, tmp := range m.ReduceList {
				if tmp.State == NOTASSIGNED {
					reply.Task = tmp
					tmp.State = ASSIGNED
					tmp.Type = TaskReduce
					flag = true
					break
				}
			}
			if flag == false {
				t.Type = TaskWait
				reply.Task = t
			}
		}
	} else if args.MessageType == FinishTask {
		if args.Task.Type == TaskMap {
			for _, f := range args.Task.Filename {
				strArr := strings.Split(f, "-")
				idx, _ := strconv.Atoi(strArr[1])
				reduceidx, _ := strconv.Atoi(strArr[2])
				m.MapList[idx].State = FINISHED
				m.MapList[idx].Type = TaskEnd
				fmt.Println(reduceidx, idx, "map", f, strArr)
				m.MapDone -= 1
				m.ReduceList[reduceidx].Filename = append(m.ReduceList[reduceidx].Filename, f)
			}
		} else {
			m.ReduceDone -= 1
			m.ReduceList[args.Task.Index].State = FINISHED
			m.ReduceList[args.Task.Index].Type = TaskEnd
			fmt.Println(m.ReduceDone, args.MessageType, "FinishTask")
		}
		fmt.Println(args.MessageType, "FinishTask")
	}

	m.mu.Unlock()

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	ret := false
	// Your code here.

	m.mu.Lock()
	ret = m.ReduceDone == 0 && m.MapDone == 0
	m.mu.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	cnt := 0
	for i, filename := range files {
		s := []string{filename}
		m.MapList = append(m.MapList, MapReduceTask{i, TaskMap, NOTASSIGNED, s, time.Now(), nReduce})
		cnt += 1
	}
	for i := 0; i < nReduce; i++ {
		m.ReduceList = append(m.ReduceList, MapReduceTask{i, TaskReduce, NOTASSIGNED, []string{}, time.Now(), cnt})

	}
	m.MapDone = cnt
	m.ReduceDone = nReduce
	m.server()
	return &m
}
