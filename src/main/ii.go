package main

import "os"
import "fmt"
import "../mapreduce"
import "sort"
import "strconv"
import "strings"

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// TODO: you should complete this to do the inverted index challenge
	strArray := strings.FieldsFunc(value, func(c rune) bool {
		switch {
		case 'a' <= c && c <= 'z':
			return false
		case 'A' <= c && c <= 'Z':
			return false
		default:
			return true
		}
	})
	var tempMap map[string]string
	tempMap = make(map[string]string)
	for _, word := range strArray {
		if _, ok := tempMap[word]; !ok {
			tempMap[word] = document
		}
	}
	for k, v := range tempMap {
		var tempkv mapreduce.KeyValue
		tempkv.Key = k
		tempkv.Value = v
		res = append(res, tempkv)
	}
	return
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// TODO: you should complete this to do the inverted index challenge
	var count int
	count = len(values)
	sort.Strings(values)
	var result string
	result = strconv.Itoa(count) + " " + strings.Join(values, ",")
	//log.Print("values:	", values)
	return result
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
