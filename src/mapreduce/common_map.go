package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	inFile_content_raw, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("read content file %s failed on doMap Task %d: ", inFile, mapTaskNumber, err)
	}
	inFile_content := string(inFile_content_raw)
	words_count := mapF(inFile, inFile_content)
	/*
		for _, kv := range words_count {
			log.Print("k:	", kv.Key, "v:	", kv.Value)
		}
	*/
	var reduceMap map[uint32][]KeyValue
	reduceMap = make(map[uint32][]KeyValue)
	for _, wordsKV := range words_count {
		var rIndex uint32 = ihash(wordsKV.Key) % uint32(nReduce)
		if KVArray, ok := reduceMap[rIndex]; ok {
			reduceMap[rIndex] = append(KVArray, wordsKV)
		} else {
			var tempA []KeyValue
			tempA = append(tempA, wordsKV)
			reduceMap[rIndex] = tempA
		}
	}
	/*
		for tempk, tempv := range reduceMap {
			log.Print("reduceMap key: ", tempk, "value: ", tempv)
		}
	*/
	for i := 0; i < nReduce; i++ {
		file_name := reduceName(jobName, mapTaskNumber, i)
		f, err := os.Create(file_name)
		if err != nil {
			log.Fatal("create intermedia file %d failed", file_name, err)
		}
		enc := json.NewEncoder(f)
		if KVArray, ok := reduceMap[uint32(i)]; ok {
			var temp_test int
			temp_test = 0
			for _, kv := range KVArray {
				err := enc.Encode(kv)
				if err != nil {
					log.Fatal("encode error:", err)
				}
				temp_test++
			}
			//log.Print("foristkirito KVArray size:%d", temp_test)
		}
		f.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
