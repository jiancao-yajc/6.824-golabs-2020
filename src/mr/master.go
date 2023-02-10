package mr

import (
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

	MapDone    bool
	ReduceDone bool
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) handler(args *Taskargs, reply *TaskReply) error {
	m.mu.Lock()
	var t MapReduceTask
	if m.MapDone && m.ReduceDone {
		t.Type = TaskEnd
	} else if args.MessageType == RequestTask {
		if m.MapDone == false {
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
		} else {
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
				idx, _ := strconv.Atoi(strArr[2])
				m.ReduceList[idx].Filename = append(m.ReduceList[idx].Filename, f)
			}
		} else {
			m.ReduceList[args.Task.Index].State = FINISHED
			m.ReduceList[args.Task.Index].Type = TaskEnd
		}

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
	ret := false
	// Your code here.

	m.mu.Lock()
	ret = m.ReduceDone && m.MapDone
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
	m.server()
	return &m
}
