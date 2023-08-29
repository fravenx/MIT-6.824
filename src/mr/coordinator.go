package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	mutex       sync.Mutex
	mapIndex    int
	reduceIndex int
	files       []string
	map1        map[int]bool //false 未完成且计时不到10s true 未完成且计时已过10s
	reducePhase bool
	nReduce     int
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Println("Coordinator Example excuted")
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) ReduceNum(args *AskReduceNumArgs, reply *AskReduceNumReply) error {
	reply.ReduceNum = c.nReduce
	return nil
}

func (c *Coordinator) AskReduce(args *AskReduceNumArgs, reply *AskReduceNumReply) error {
	if c.reduceIndex == c.nReduce {
		return nil
	}
	reply.ReduceNum = c.reduceIndex
	c.reduceIndex++
	return nil
}

func (c *Coordinator) Asktask(args *AskTaskArgs, reply *AskTaskReply) error {
	fmt.Println("Coordinator asktast excuted")
	if c.reducePhase {
		reply.SartReduce = true
		return nil
	}

	if c.mapIndex < len(c.files) {
		reply.Task = c.files[c.mapIndex]
		c.map1[c.mapIndex] = false
		i := c.mapIndex
		go c.waitWorker(i)
		c.mapIndex++
	} else if len(c.map1) > 0 {
		key := -1
		for k, v := range c.map1 {
			if v {
				key = k
				break
			}
		}
		reply.Task = c.files[key]
		c.map1[c.mapIndex] = false
		i := c.mapIndex
		go c.waitWorker(i)
	} else {
		c.reducePhase = true
		reply.SartReduce = true
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
	go http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		http.DefaultServeMux.ServeHTTP(w, r)
	}))
}

func (c *Coordinator) waitWorker(mapId int) {
	time.Sleep(10 * time.Second)
	if c.map1[mapId] {
		c.map1[mapId] = true
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files_ []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapIndex = 0
	c.reduceIndex = 0
	c.files = files_
	c.reducePhase = false
	c.nReduce = nReduce
	c.server()
	return &c
}
