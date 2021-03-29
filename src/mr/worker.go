package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		args := Taskargs{MessageType: RequestTask}
		reply := TaskReply{}
		res := call("Master.handler", &args, &reply)
		if !res {
			break
		}

		switch reply.Task.Type {
		case TaskMap:
			doMap(&reply, mapf)
		case TaskReduce:
			doReduce(&reply, reducef)
		case TaskWait:
			time.Sleep(1 * time.Second)
		case TaskEnd:
			break
		}

	}

	// send the RPC request, wait for the reply.

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func doMap(r *TaskReply, mapf func(string, string) []KeyValue) {

	intermediate := []KeyValue{}
	filename := r.Task.Filename[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	args := Taskargs{MessageType: FinishTask}
	args.Task.State = TaskMap

	for _, kv := range intermediate {

		newName := "mr-" + strconv.Itoa(r.Task.Index) + "-" + strconv.Itoa(ihash(kv.Key)%r.Task.ReduceNumber)

		tmpfile, err := ioutil.TempFile("", "tmp")
		if err != nil {
			log.Fatal(err)
		}

		tmpfile.Close()

		// Error string: "Cannot create a file when that file already exists."
		if err := os.Rename(tmpfile.Name(), newName); err != nil {
			log.Printf("Rename error: %v", err)

		}
		file, _ := os.Open(newName)

		enc := json.NewEncoder(file)
		err = enc.Encode(&kv)
		args.Task.Filename = append(args.Task.Filename, newName)
	}

	reply := TaskReply{}
	call("Master.handler", &args, &reply)

}
func doReduce(r *TaskReply, reducef func(string, []string) string) {
	args := Taskargs{MessageType: FinishTask}
	args.Task.State = TaskReduce
	for _, filename := range r.Task.Filename {
		strArr := strings.Split(filename, "-")
		file, _ := os.Open("filename")
		intermediate := []KeyValue{}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		sort.Sort(ByKey(intermediate))
		newName := "mr-out-" + strArr[2]
		tmpfile, err := ioutil.TempFile("", "tmp")
		if err != nil {
			log.Fatal(err)
		}

		tmpfile.Close()

		// Error string: "Cannot create a file when that file already exists."
		if err := os.Rename(tmpfile.Name(), newName); err != nil {
			log.Printf("Rename error: %v", err)

		}
		ofile, err := os.Open(newName) // For read access.
		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
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
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		ofile.Close()
	}
	reply := TaskReply{}
	call("Master.handler", &args, &reply)
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
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
