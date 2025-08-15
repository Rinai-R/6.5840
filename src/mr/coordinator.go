package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

const (
	UnAssigned = 0
	Assigned   = 1
	Done       = 2
)

type Coordinator struct {
	MapWork          []string
	MapWorkStatus    []int
	ReduceWorkStatus []int
	NReduce          int
	MapMu            sync.Mutex
	ReduceMu         sync.Mutex
	MapDoneNum       atomic.Int32
	ReduceDoneNum    atomic.Int32
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	if c.ReduceDoneNum.Load() == int32(c.NReduce) {
		reply.Type = "done"
		return nil
	}
	if c.MapDoneNum.Load() < int32(len(c.MapWork)) {
		// 分配 map 任务
		c.MapMu.Lock()
		for i := 0; i < len(c.MapWorkStatus); i++ {
			if c.MapWorkStatus[i] == UnAssigned {
				c.MapWorkStatus[i] = Assigned
				reply.Type = "map"
				reply.WorkId = i
				reply.FileName = c.MapWork[i]
				reply.NReduce = c.NReduce
				c.MapMu.Unlock()
				return nil
			}
		}
		c.MapMu.Unlock()
	}
	if c.ReduceDoneNum.Load() < int32(len(c.MapWork)) {
		// 分配 reduce 任务
		c.ReduceMu.Lock()
		for i := 0; i < len(c.ReduceWorkStatus); i++ {
			if c.ReduceWorkStatus[i] == UnAssigned {
				c.ReduceWorkStatus[i] = Assigned
				reply.Type = "reduce"
				reply.WorkId = i
				reply.NMap = len(c.MapWork)
				c.ReduceMu.Unlock()
				return nil
			}
		}
		c.ReduceMu.Unlock()
	}
	reply.Type = "wait"
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	switch args.Type {
	case "map":
		if args.Status == "done" {
			c.MapMu.Lock()
			c.MapWorkStatus[args.WorkId] = Done
			c.MapDoneNum.Add(1)
			c.MapMu.Unlock()
		} else {
			c.MapMu.Lock()
			c.MapWorkStatus[args.WorkId] = UnAssigned
			c.MapMu.Unlock()
		}
	case "reduce":
		if args.Status == "done" {
			c.ReduceMu.Lock()
			c.ReduceWorkStatus[args.WorkId] = Done
			c.ReduceDoneNum.Add(1)
			c.ReduceMu.Unlock()
		} else {
			c.ReduceMu.Lock()
			c.ReduceWorkStatus[args.WorkId] = UnAssigned
			c.ReduceMu.Unlock()
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	return c.ReduceDoneNum.Load() == int32(c.NReduce)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapWork:          files,
		MapWorkStatus:    make([]int, len(files)),
		ReduceWorkStatus: make([]int, nReduce),
		NReduce:          nReduce,
		MapMu:            sync.Mutex{},
		ReduceMu:         sync.Mutex{},
		ReduceDoneNum:    atomic.Int32{},
		MapDoneNum:       atomic.Int32{},
	}

	c.server()
	return &c
}
