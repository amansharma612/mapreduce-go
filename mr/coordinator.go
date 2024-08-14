package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "sync"
// import "fmt"
import "time"
import "strings"



var numworkers int = 0
var done bool = false

type LiveJob struct {

	WorkerID string
	Filename string
	JobType string
	WorkID int
	TimeStarted time.Time

}


type Coordinator struct {
	// Your definitions here.
	MapJobsAssigned []int
	MapJobsFinished []int
	ReduceJobsAssigned []int
	ReduceJobsFinished []int
	JobStatus []LiveJob
	FileNames []string
	NReduce int
	FilesMap map[int][]string
	mu sync.Mutex
	
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) WorkInit(args *ExampleArgs, reply *WorkerInitReply) error {
	c.mu.Lock()
	numworkers = numworkers + 1
	reply.WorkerID = strconv.Itoa(numworkers)
	c.mu.Unlock()
	return nil
}


func (c *Coordinator) WorkQuery(args *WorkQueryRequest, reply *WorkQueryReply) error {
	c.mu.Lock()
	
	for i := range c.MapJobsAssigned {
		if c.MapJobsAssigned[i] == 0 {
			
			c.MapJobsAssigned[i] = 1
			reply.Filename = c.FileNames[i]
			reply.Reqtype = "map"
			reply.WorkID = i
			reply.NReduce = c.NReduce
			
			livejob := LiveJob{}
			livejob.Filename = reply.Filename
			livejob.JobType = reply.Reqtype
			livejob.WorkerID = args.WorkerID
			livejob.WorkID = i
			livejob.TimeStarted = time.Now()
			
			c.JobStatus = append(c.JobStatus, livejob)
			
			c.mu.Unlock()
			return nil
		}
	}

	for i := range c.MapJobsFinished {
		if c.MapJobsFinished[i] == 0 {
			c.mu.Unlock()
			return nil
		}
	}

	// All Map Jobs have been finished till this point
	for i := range c.ReduceJobsAssigned {
		if c.ReduceJobsAssigned[i] == 0 {
			
			c.ReduceJobsAssigned[i] = 1
			reply.Filename = "reduce-" + args.WorkerID + "-" + strconv.Itoa(i)
			reply.Reqtype = "reduce"
			reply.WorkID = i
			reply.FileNames = c.FilesMap[i]
			

			livejob := LiveJob{}
			livejob.Filename = reply.Filename
			livejob.JobType = reply.Reqtype
			livejob.WorkerID = args.WorkerID
			livejob.WorkID = i
			livejob.TimeStarted = time.Now()
			
			c.JobStatus = append(c.JobStatus, livejob)
			break
		}
	}

	for i := range c.ReduceJobsFinished {
		if c.ReduceJobsFinished[i] == 0 {
			c.mu.Unlock()
			return nil
		}
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) WorkComplete(args *WorkCompleteRequest, reply *WorkCompleteReply) error {
	
	reply.Ack = "Completed!"
	if args.Reqtype == "map" && c.MapJobsFinished[args.WorkID] == 1 {
		return nil
	} else if args.Reqtype == "reduce" && c.ReduceJobsFinished[args.WorkID] == 1 {
		return nil
	}

	c.mu.Lock()
	for i := range c.JobStatus {
		if args.WorkID == c.JobStatus[i].WorkID && args.Reqtype == c.JobStatus[i].JobType {
			
			c.JobStatus = append(c.JobStatus[:i], c.JobStatus[i+1:]...)
			if args.Reqtype == "map" {
				c.MapJobsFinished[args.WorkID] = 1
				
				for _, filename := range args.FileNames {
					t := strings.Split(filename, "-")
					j, _ := strconv.Atoi(t[2])
					c.FilesMap[j] = append(c.FilesMap[j], filename)	
				}

			} else {
				c.ReduceJobsFinished[args.WorkID] = 1
				newname := "mr-out-" + strconv.Itoa(args.WorkID)
				os.Rename(args.Filename, newname)

			}

			break
		}
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) JobChecker() error {
	for true {
		c.mu.Lock();
		currtime := time.Now()
		for i := range c.JobStatus {
			if currtime.Sub(c.JobStatus[i].TimeStarted).Seconds() > 10 {
				if c.JobStatus[i].JobType == "map" {
					c.MapJobsAssigned[c.JobStatus[i].WorkID] = 0
				} else {
					c.ReduceJobsAssigned[c.JobStatus[i].WorkID] = 0
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(10 * time.Second)
	}

	return nil
}




//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapJobsAssigned = make ([]int, len(files))
	c.MapJobsFinished = make ([]int, len(files))
	c.ReduceJobsAssigned = make ([]int, nReduce)
	c.ReduceJobsFinished = make ([]int, nReduce)
	
	for i := range c.MapJobsAssigned {
		c.MapJobsAssigned[i] = 0
		c.MapJobsFinished[i] = 0
	} 

	for i := 0; i < nReduce; i++ {
		c.ReduceJobsAssigned[i] = 0
		c.ReduceJobsFinished[i] = 0
	}

	c.FileNames = files
	c.NReduce = nReduce
	c.FilesMap = make(map[int][]string)
	


	c.server()
	c.JobChecker()
	return &c
}
