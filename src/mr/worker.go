package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

// for sorting by key.
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var nReduce int
	args1 := AskReduceNumArgs{}
	reply1 := AskReduceNumReply{}
	ok := call("Coordinator.ReduceNum", &args1, &reply1)
	if !ok {
		fmt.Printf("call reducenum error")
	}
	nReduce = reply1.ReduceNum

	args := AskTaskArgs{}
	reply := AskTaskReply{}
	reply.Task = ""
	reply.SartReduce = false
	ok = call("Coordinator.Asktask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	fmt.Printf("asktast reply task = %v", reply.Task)

	if reply.Task != "" {
		// read file
		filename := "../main/" + reply.Task
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

		for i := 0; i < nReduce; i++ {
			intermediateFile_ := "mr-" + strings.Replace(reply.Task[:len(reply.Task)-4], "-", "-", -1) + "-" + strconv.Itoa(i)
			_, err := os.Create(intermediateFile_)
			if err != nil {
				fmt.Println("os create file error")
				return
			}
		}

		// write to intermediate files
		for _, kv := range kva {
			reduceNum := ihash(kv.Key)
			intermediateFile_ := "mr-" + strings.Replace(reply.Task[:len(reply.Task)-4], "-", "-", -1) + "-" + strconv.Itoa(reduceNum)
			file, err = os.Open(intermediateFile_)
			enc := json.NewEncoder(file)
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("encode failed!\n")
			}
			file.Close()
		}

		// send func map success message
	} else if reply.SartReduce {
		// start to reduce
		args2 := AskReduceArgs{}
		reply2 := AskReduceReply{}
		reply2.ReduceNum = -1
		ok := call("Coordinator.AskReduce", &args2, &reply2)
		if !ok {
			fmt.Printf("ask reduce error")
		}
		if reply2.ReduceNum < 0 {
			return
		}

		reduceNum := reply2.ReduceNum
		dir := "."
		suffix := strconv.Itoa(reduceNum)
		var kva []KeyValue
		intermediate := []KeyValue{}
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(path, suffix) {
				file, _ := os.Open(path)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				intermediate = append(intermediate, kva...)
			}
			return nil
		})
		if err != nil {
			fmt.Println(err)
		}

		sort.Sort(ByKey(intermediate))

		oname := "mr-out-" + strconv.Itoa(reduceNum)
		ofile, _ := os.Create(oname)
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
		//send reduce success message
	} else {
		//wait for a period of time

	}

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
