package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	for i := 0; ; i++ {
		reply, err := CallAssignTask()
		if err != nil {
			fmt.Println(err)
			continue
		}
		switch reply.Type {
		case "map":
			data, err := os.ReadFile(reply.FileName)
			if err != nil {
				req := &ReportTaskArgs{
					Type:   "map",
					WorkId: reply.WorkId,
					Status: "failed",
				}
				CallReportTask(req)
				continue
			}
			kvs := mapf(reply.FileName, string(data))

			// 执行写文件逻辑
			err = doMap(kvs, reply.NReduce, reply.WorkId)
			if err != nil {
				req := &ReportTaskArgs{
					Type:   "map",
					WorkId: reply.WorkId,
					Status: "failed",
				}
				CallReportTask(req)
				continue
			}
			req := &ReportTaskArgs{
				Type:   "map",
				WorkId: reply.WorkId,
				Status: "done",
			}
			CallReportTask(req)
		case "reduce":
			// 执行文件读取并写入的操作
			err = doReduce(reply.WorkId, reply.NReduce, reply.NMap, reducef)
			if err != nil {
				req := &ReportTaskArgs{
					Type:   "reduce",
					WorkId: reply.WorkId,
					Status: "failed",
				}
				CallReportTask(req)
				continue
			}
			req := &ReportTaskArgs{
				Type:   "reduce",
				WorkId: reply.WorkId,
				Status: "done",
			}
			CallReportTask(req)
		case "wait":
			time.Sleep(1 * time.Second)
		case "done":
			return
		}
	}
}

func doMap(kvs []KeyValue, nReduce int, workId int) error {
	buckets := make([][]KeyValue, nReduce)
	// 先分桶
	for _, kv := range kvs {
		idx := ihash(kv.Key) % nReduce
		buckets[idx] = append(buckets[idx], kv)
	}
	type out struct {
		f   *os.File
		bw  *bufio.Writer
		enc *json.Encoder
		tmp string
	}
	outs := make([]out, nReduce)

	// 提前创建所有分区的临时文件和编码器
	for y := 0; y < nReduce; y++ {
		tmp := fmt.Sprintf("mr-%d-%d.tmp", workId, y)
		f, err := os.Create(tmp)
		if err != nil {
			// 清理已创建的
			for i := 0; i < y; i++ {
				outs[i].f.Close()
				os.Remove(outs[i].tmp)
			}
			return err
		}
		bw := bufio.NewWriterSize(f, 256<<10)
		outs[y] = out{
			f:   f,
			bw:  bw,
			enc: json.NewEncoder(bw),
			tmp: tmp,
		}
	}

	// 逐个分区写入临时文件
	for y := 0; y < nReduce; y++ {
		for _, kv := range buckets[y] {
			if err := outs[y].enc.Encode(&kv); err != nil {
				// 写失败时清理所有已创建文件
				for _, o := range outs {
					if o.f != nil {
						o.f.Close()
						os.Remove(o.tmp)
					}
				}
				return err
			}
		}
	}

	// 完成写入，原子命名
	for y := 0; y < nReduce; y++ {
		o := outs[y]
		if err := o.bw.Flush(); err != nil {
			o.f.Close()
			os.Remove(o.tmp)
			return err
		}
		if err := o.f.Close(); err != nil {
			os.Remove(o.tmp)
			return err
		}
		final := fmt.Sprintf("mr-%d-%d", workId, y)
		if err := os.Rename(o.tmp, final); err != nil {
			os.Remove(o.tmp)
			return err
		}
	}
	return nil
}

func doReduce(wordId int, nReduce int, nMap int, reducef func(string, []string) string) error {
	groups := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		name := fmt.Sprintf("mr-%d-%d", i, wordId)
		if _, err := os.Stat(name); os.IsNotExist(err) {
			continue
		}
		f, _ := os.Open(name)
		dec := json.NewDecoder(bufio.NewReader(f))
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			groups[kv.Key] = append(groups[kv.Key], kv.Value)
		}
		f.Close()
	}

	// 排序一下
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 临时文件
	tmp := fmt.Sprintf("mr-out-%d.tmp", wordId)
	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("创建临时输出失败: %w", err)
	}
	bw := bufio.NewWriterSize(out, 256<<10)

	for _, k := range keys {
		res := reducef(k, groups[k])
		// 一行一行写入
		if _, err := fmt.Fprintf(bw, "%s %s\n", k, strings.TrimSpace(res)); err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("写出结果失败: %w", err)
		}
	}

	if err := bw.Flush(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("刷新缓冲失败: %w", err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("关闭输出失败: %w", err)
	}

	final := fmt.Sprintf("mr-out-%d", wordId)
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("改名失败: %w", err)
	}
	return nil
}

func CallAssignTask() (*AssignTaskReply, error) {
	AssignArgs := &AssignTaskArgs{}
	reply := &AssignTaskReply{}
	ok := call("Coordinator.AssignTask", AssignArgs, reply)
	if !ok {
		return nil, fmt.Errorf("AssignTask failed")
	}
	return reply, nil
}

func CallReportTask(req *ReportTaskArgs) bool {
	ok := call("Coordinator.ReportTask", req, nil)
	return ok
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
