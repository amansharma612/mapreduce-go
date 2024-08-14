package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "errors"
import "sort"



//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}


type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator

	workerID := CallWorkerInit();

	for true {
		reply := CallWorkQuery(workerID);
		filemap := map[string]int{}


		if reply.Reqtype == "map" {
			fmt.Printf("%v\n",reply.WorkID)
			file, err := os.Open(reply.Filename)
			if err != nil {
				fmt.Printf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				fmt.Printf("cannot read %v", reply.Filename)
			}
			file.Close()
			
			if err == nil {

				kva := mapf(reply.Filename, string(content))

				for i := 0; i < reply.NReduce; i++ {
					totrunc := "mr-" + strconv.Itoa(reply.WorkID) + "-" + strconv.Itoa(i)
					if err := os.Truncate(totrunc, 0); err != nil {
						log.Printf("Failed to truncate: %v", err)
					}
					filemap[totrunc] = 1
				}

				for i := range kva {
					// fmt.Printf("%v", reply.NReduce)
					filenum := ihash(kva[i].Key) % reply.NReduce
					writenm := "mr-" + strconv.Itoa(reply.WorkID) + "-" + strconv.Itoa(filenum)
					

					if _, err = os.Stat(writenm); errors.Is(err, os.ErrNotExist) {
						// file does not exist
						os.Create(writenm)
					}
					file1, err := os.OpenFile(writenm, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
					// fmt.Printf("Opened file %v\n", writenm)
					if err != nil {
						fmt.Printf("cannot open %v\n", writenm)
					}	
					
					enc := json.NewEncoder(file1)
					kv := kva[i]
					err = enc.Encode(&kv)
					if err != nil {
						fmt.Printf("Why error? %v", err)
					}
					
					file1.Close()
					// fmt.Printf("written in file %v\n", writenm)
				}


				filelist := []string{}
				
				for key, _ := range filemap {
					filelist = append(filelist, key)
				}
				
				CallWorkComplete(workerID, reply.Reqtype, reply.WorkID, filelist, "dummy")

			}
			
		} else if reply.Reqtype == "reduce" {
			intermediate := []KeyValue{}
			ofile, _ := os.Create(reply.Filename)

			for _, imfile := range reply.FileNames {
				file, err := os.OpenFile(imfile, os.O_RDONLY, 0)
				if err != nil {
					fmt.Printf("Error in opening file: %v\n", err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))
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
			CallWorkComplete(workerID, reply.Reqtype, reply.WorkID, reply.FileNames, reply.Filename)
			
		}

		time.Sleep(time.Second);
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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


func CallWorkerInit() string{

	// declare a reply structure.
	args := ExampleArgs{}
	args.X  = 88
	reply := WorkerInitReply{}

	ok := call("Coordinator.WorkInit", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Recieved worker ID %v\n", reply.WorkerID)
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(1);
	}

	

	return reply.WorkerID
}

func CallWorkQuery(workerID string) WorkQueryReply {

	// declare an argument structure.
	args := WorkQueryRequest{}

	// fill in the argument(s).
	args.WorkerID = workerID

	// declare a reply structure.
	reply := WorkQueryReply{}

	ok := call("Coordinator.WorkQuery", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("WorkQuery.Filename %v\n", reply.Filename)
		fmt.Printf("WorkQuery.Reqtype %v\n", reply.Reqtype)
	
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply
	
}


func CallWorkComplete(WorkerID string, Reqtype string, WorkID int, Filelist []string, Filename string) WorkCompleteReply {

	// declare an argument structure.
	args := WorkCompleteRequest{}

	// fill in the argument(s).
	args.WorkID = WorkID
	args.Reqtype = Reqtype
	args.WorkerID = WorkerID
	args.FileNames = Filelist
	args.Filename = Filename

	// declare a reply structure.
	reply := WorkCompleteReply{}


	ok := call("Coordinator.WorkComplete", &args, &reply)
	if ok {
		fmt.Printf("WorkCompleteReply.Ack %v\n", reply.Ack)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply
	
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
