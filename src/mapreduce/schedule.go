package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var taskWait sync.WaitGroup
	taskWait.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		var tempArgs DoTaskArgs
		tempArgs.JobName = mr.jobName
		tempArgs.Phase = phase
		tempArgs.TaskNumber = i
		tempArgs.NumOtherPhase = nios
		if phase == mapPhase {
			tempArgs.File = mr.files[i]
		}
		go func() {
			for {
				var workerIdl string
				workerIdl = <-mr.registerChannel
				var re struct{}
				done := call(workerIdl, "Worker.DoTask", tempArgs, &re)
				if done {
					taskWait.Done()
					mr.registerChannel <- workerIdl
					break
				}
			}
		}()
	}
	taskWait.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
