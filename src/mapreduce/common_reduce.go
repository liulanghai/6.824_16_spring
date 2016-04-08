package mapreduce

import (
	"encoding/json"
	"log"
	"os"
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
	var kvMap map[string][]string
	kvMap = make(map[string][]string)
	for i := 0; i < nMap; i++ {
		imFileName := reduceName(jobName, i, reduceTaskNumber)
		imFile, err := os.Open(imFileName)
		if err != nil {
			log.Fatal("read intermedia file: ", err)
		}
		dec := json.NewDecoder(imFile)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			//log.Print("done")
			if valueArray, ok := kvMap[kv.Key]; ok {
				kvMap[kv.Key] = append(valueArray, kv.Value)
			} else {
				var tempkv []string
				tempkv = append(tempkv, kv.Value)
				kvMap[kv.Key] = tempkv
			}
		}
		imFile.Close()
	}
	fileName := mergeName(jobName, reduceTaskNumber)
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal("create reduce target file failed:", err)
	}
	enc := json.NewEncoder(f)
	for k, v := range kvMap {
		enc.Encode(KeyValue{k, reduceF(k, v)})
		//enc.Encode(KeyValue{k, "1"})
	}
	f.Close()
}
