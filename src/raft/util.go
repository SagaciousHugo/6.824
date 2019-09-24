package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Max(params ...int) int {
	if len(params) < 1 {
		panic("at least one param")
	}
	max := params[0]
	for i := 1; i < len(params); i++ {
		if params[i] > max {
			max = params[i]
		}
	}
	return max
}

func Min(params ...int) int {
	if len(params) < 1 {
		panic("at least one param")
	}
	min := params[0]
	for i := 1; i < len(params); i++ {
		if params[i] < min {
			min = params[i]
		}
	}
	return min
}
