package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}



// Add your RPC definitions here.
type WorkerInitReply struct {
	WorkerID string
}



type WorkQueryRequest struct {
	WorkerID string
}



type WorkQueryReply struct {
	Filename string
	WorkID int
	Reqtype string
	NReduce int
	FileNames []string
}

type WorkCompleteRequest struct {
	FileNames []string
	WorkerID string
	Reqtype string
	WorkID int
	Filename string
}

type WorkCompleteReply struct {
	Ack string
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
