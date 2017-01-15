package mapreduce

import (
	//"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	//"log"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	f, _ := os.Create(mergeName(jobName, reduceTaskNumber))
	w := json.NewEncoder(f)
	var tmp = make(map[string][]string)

	for i := 0; i < nMap; i++ {
		ff, _ := os.Open(reduceName(jobName, i, reduceTaskNumber))
		fmt.Println("reduce == " + reduceName(jobName, i, reduceTaskNumber))
		in := json.NewDecoder(ff)
		var vv KeyValue
		for {
			if err := in.Decode(&vv); err == io.EOF {
				break
			}
			v, ok := tmp[vv.Key]
			if !ok {
				arr := make([]string, 1)
				arr[0] = vv.Value
				tmp[vv.Key] = arr
			} else {
				v = append(v, vv.Value)
				tmp[vv.Key] = v
			}
		}
	}

	var keys []string
	for k, _ := range tmp {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := tmp[k]
		s := reduceF(k, v)
		var kv KeyValue = KeyValue{k, s}
		w.Encode(kv)
	}

	f.Close()
}
