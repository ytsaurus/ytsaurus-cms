package main

import "runtime"

// Adjust adjust the maximum number of CPUs that can be executing.
func Adjust() int {
	n := runtime.NumCPU()
	runtime.GOMAXPROCS(n)

	return n
}
