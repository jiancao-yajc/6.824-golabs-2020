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
	for {
		fmt.Println("1212")
		args := Taskargs{MessageType: RequestTask}
		reply := TaskReply{}
		res := call("Master.Handler", &args, &reply)
		fmt.Println(res)
		if !res {
			break
		}
		fmt.Println(reply.Task.Type)
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
	fmt.Println("domap")
	// intermediate := []KeyValue{}
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
	// intermediate = append(intermediate, kva...)
	kvas := make([][]KeyValue, r.Task.ReduceNumber)
	for _, kv := range kva {
		v := ihash(kv.Key) % r.Task.ReduceNumber
		kvas[v] = append(kvas[v], kv)
	}

	args := Taskargs{MessageType: FinishTask}
	args.Task.State = TaskMap
	args.Task.Type = TaskMap
	for i := 0; i < r.Task.ReduceNumber; i++ {
		filename := WriteToJSONFile(kvas[i], r.Task.Index, i)
		args.Task.Filename = append(args.Task.Filename, filename)
	}
	// for _, kv := range intermediate {

	// 	newName := "mr-" + strconv.Itoa(r.Task.Index) + "-" + strconv.Itoa(ihash(kv.Key)%r.Task.ReduceNumber)
	// 	fmt.Println(kv, newName)
	// 	// tmpfile, err := ioutil.TempFile("", "tmp")
	// 	// if err != nil {
	// 	// 	log.Fatal(err)
	// 	// }

	// 	// tmpfile.Close()

	// 	// Error string: "Cannot create a file when that file already exists."
	// 	// if err := os.Replace(tmpfile.Name(), newName); err != nil {
	// 	// 	log.Printf("Rename error: %v", err)

	// 	// }

	// }
	fmt.Println("domap done")
	reply := TaskReply{}

	call("Master.Handler", &args, &reply)

}

// WriteToJSONFile : write intermediate KeyValue pairs to a Json file
func WriteToJSONFile(intermediate []KeyValue, mapTaskNum, reduceTaskNUm int) string {
	filename := "mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(reduceTaskNUm)
	jfile, _ := os.Create(filename)

	enc := json.NewEncoder(jfile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
	return filename
}

func doReduce(r *TaskReply, reducef func(string, []string) string) {
	fmt.Println("doReduce")
	args := Taskargs{MessageType: FinishTask}
	args.Task.State = TaskReduce
	args.Task.Type = TaskReduce
	intermediate := []KeyValue{}
	for _, filename := range r.Task.Filename {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		sort.Sort(ByKey(intermediate))
	}
	newName := "mr-out-" + strconv.Itoa(args.Task.Index)
	fmt.Println(newName)
	// tmpfile, err := ioutil.TempFile("", "tmp")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// tmpfile.Close()

	// Error string: "Cannot create a file when that file already exists."
	// if err := os.Rename(tmpfile.Name(), newName); err != nil {
	// 	log.Printf("Rename error: %v", err)

	// }
	ofile, _ := os.Create(newName)
	// ofile, err := os.Open(newName) // For read access.
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

	reply := TaskReply{}
	call("Master.Handler", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	fmt.Println(sockname)
	c, err := rpc.DialHTTP("unix", sockname)
	time.Sleep(time.Second * 1)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	fmt.Println("Call")
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println("Done")
	fmt.Println(err)
	return false
}
