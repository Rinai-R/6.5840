package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	UnAssigned = 0
	Assigned   = 1
	Done       = 2
)

type Coordinator struct {
	MapTask       []Task
	ReduceTask    []Task
	NReduce       int
	Mu            sync.Mutex
	MapDoneNum    atomic.Int32
	ReduceDoneNum atomic.Int32
	LastAssignId  atomic.Int32
}

type Task struct {
	FileName string
	Index    int
	Type     string
	Status   int
	OutTime  time.Time
	AcceptId int32
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	if c.ReduceDoneNum.Load() == int32(c.NReduce) {
		reply.Type = "done"
		return nil
	}
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if c.MapDoneNum.Load() < int32(len(c.MapTask)) {
		// 分配 map 任务
		for i := 0; i < len(c.MapTask); i++ {
			if c.MapTask[i].Status == UnAssigned {
				c.LastAssignId.Add(1)
				c.MapTask[i].AcceptId = c.LastAssignId.Load()
				c.MapTask[i].Status = Assigned
				c.MapTask[i].OutTime = time.Now().Add(10 * time.Second)

				reply.Type = "map"
				reply.WorkId = i
				reply.FileName = c.MapTask[i].FileName
				reply.NReduce = c.NReduce
				reply.AcceptId = c.MapTask[i].AcceptId
				return nil
			}
		}
	}
	if c.ReduceDoneNum.Load() < int32(len(c.ReduceTask)) {
		// 分配 reduce 任务
		for i := 0; i < len(c.ReduceTask); i++ {
			if c.ReduceTask[i].Status == UnAssigned {
				c.LastAssignId.Add(1)
				c.ReduceTask[i].AcceptId = c.LastAssignId.Load()
				c.ReduceTask[i].Status = Assigned
				c.ReduceTask[i].OutTime = time.Now().Add(10 * time.Second)
				reply.Type = "reduce"
				reply.WorkId = i
				reply.NMap = len(c.MapTask)

				return nil
			}
		}
	}
	reply.Type = "wait"
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	switch args.Type {
	case "map":
		if c.MapTask[args.WorkId].Status != Assigned || args.AcceptId != c.MapTask[args.WorkId].AcceptId {
			return nil
		}
		if args.Status == "done" {
			c.MapTask[args.WorkId].Status = Done
			c.MapDoneNum.Add(1)
		} else {
			c.MapTask[args.WorkId].Status = UnAssigned
		}
	case "reduce":
		// 防止 Task 重复 report
		if c.ReduceTask[args.WorkId].Status != Assigned || args.AcceptId != c.ReduceTask[args.WorkId].AcceptId {
			return nil
		}
		if args.Status == "done" {
			c.ReduceTask[args.WorkId].Status = Done
			c.ReduceDoneNum.Add(1)
		} else {
			c.ReduceTask[args.WorkId].Status = UnAssigned
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
	ReduceTask := make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		ReduceTask[i] = Task{
			FileName: "",
			Index:    i,
			Type:     "reduce",
			Status:   UnAssigned,
			OutTime:  time.Now(),
			AcceptId: -1,
		}
	}
	MapTask := make([]Task, len(files))
	for i := 0; i < len(files); i++ {
		MapTask[i] = Task{
			FileName: files[i],
			Index:    i,
			Type:     "map",
			Status:   UnAssigned,
			OutTime:  time.Now(),
			AcceptId: -1,
		}
	}
	c := Coordinator{
		MapTask:       MapTask,
		ReduceTask:    ReduceTask,
		NReduce:       nReduce,
		Mu:            sync.Mutex{},
		ReduceDoneNum: atomic.Int32{},
		MapDoneNum:    atomic.Int32{},
		LastAssignId:  atomic.Int32{},
	}
	// 回收超时的任务
	go c.CheckOutTime()
	c.server()
	return &c
}

func (c *Coordinator) CheckOutTime() {
	tick := time.NewTicker(time.Second * 5)
	for range tick.C {
		c.Mu.Lock()
		for i := 0; i < len(c.MapTask); i++ {
			if c.MapTask[i].Status == Assigned && time.Since(c.MapTask[i].OutTime) > time.Second*10 {
				// log.Printf("map task %d is out of time", i)
				c.MapTask[i].Status = UnAssigned
			}
		}
		for i := 0; i < len(c.ReduceTask); i++ {
			if c.ReduceTask[i].Status == Assigned && time.Since(c.ReduceTask[i].OutTime) > time.Second*10 {
				// log.Printf("reduce task %d is out of time", i)
				c.ReduceTask[i].Status = UnAssigned
			}
		}
		c.Mu.Unlock()
	}
}
