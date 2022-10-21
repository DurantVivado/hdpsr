package hdpsr

import "container/heap"

// A simple implementation of small top HeapInt
type HeapInt []int

func (h HeapInt) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h HeapInt) Less(i, j int) bool {
	return h[i] < h[j] // change "<" to ">" if you want a big top heap
}

func (h HeapInt) Len() int {
	return len(h)
}

func (h *HeapInt) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *HeapInt) Pop() interface{} {
	h_ := *h
	n := len(h_)
	x := h_[n-1]
	*h = h_[0 : n-1]
	return x
}

func HeapSortInt(arr []int) []int {
	h := HeapInt(arr)
	heap.Init(&h)
	sortedArr := make([]int, 0)
	for len(h) > 0 {
		sortedArr = append(sortedArr, heap.Pop(&h).(int))
	}
	return sortedArr
}

// A simple realization of SMALL TOP HEAP!
type heapv struct {
	id  int
	val float64
}
type HeapFloat64 []heapv

func (h HeapFloat64) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h HeapFloat64) Less(i, j int) bool {
	return h[i].val < h[j].val
}

func (h HeapFloat64) Len() int {
	return len(h)
}

func (h *HeapFloat64) Push(x interface{}) {
	*h = append(*h, x.(heapv))
}

func (h *HeapFloat64) Pop() interface{} {
	h_ := *h
	n := len(h_)
	x := h_[n-1]
	*h = h_[0 : n-1]
	return x
}

func MakeHeap(arr []float64) (h *HeapFloat64) {
	h = &HeapFloat64{}
	for i, v := range arr {
		*h = append(*h, heapv{i, v})
	}
	return
}

func HeapSortFloat64(arr []float64) []float64 {
	h := MakeHeap(arr)
	heap.Init(h)
	sortedArr := make([]float64, 0)
	for len(*h) > 0 {
		sortedArr = append(sortedArr, heap.Pop(h).(heapv).val)
	}
	return sortedArr
}
