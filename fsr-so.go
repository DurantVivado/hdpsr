package hdpsr

import (
	"container/heap"
	"fmt"
)

func helper(A []float64, mid float64, i int, sum float64, group []int, scheme *[][]int, minTime *float64) bool {
	if i >= len(A) {
		return true
	}
	group = append(group, i)
	sum += A[i]
	for j := i + 1; j < len(A); j++ {
	}
	return false
}

func findNonContinuousScheme(A []float64, mid float64, scheme *[][]int, minTime *float64) bool {
	group := make([]int, 0)
	return helper(A, mid, 0, 0, group, scheme, minTime)
}

func findContinuousScheme(A []float64, mid float64, Pr int) (
	stripeOrder [][]int, minTime float64) {
	cnt := 0
	sum := float64(0)
	stripeOrder = make([][]int, Pr)
	maxSubTime := float64(0)
	for i := 0; i < len(A); i++ {
		if sum+A[i] > mid {
			cnt++
			minTime = maxFloat64(minTime, maxSubTime)
			sum = 0
			maxSubTime = 0
		}
		if cnt >= Pr {
			return nil, 0
		}
		sum += A[i]
		stripeOrder[cnt] = append(stripeOrder[cnt], i)
		maxSubTime += A[i]
	}
	return
}

// full-stripe-repair with stripe order first
func (e *Erasure) getMinimalTime(stripeRepairTime []float64) (
	stripeOrder [][]int, minTime float64) {
	if len(stripeRepairTime) == 0 {
		return nil, 0
	}
	Pr := (e.MemSize * GiB) / (e.K * int(e.dataStripeSize))
	fmt.Printf("Pr:%d\n", Pr)
	if len(stripeRepairTime) <= Pr {
		for i := 0; i < len(stripeRepairTime); i++ {
			stripeOrder[i] = append(stripeOrder[i], i)
		}
		return stripeOrder, maxFloat64(stripeRepairTime...)
	}
	maxTime := maxFloat64(stripeRepairTime...)
	sumTime := sumFloat64(stripeRepairTime...)
	l, r := maxTime, sumTime

	for r-l > 1e-6 {
		mid := l + (r-l)/2
		stripeOrder = make([][]int, Pr)
		ret1, ret2 := findContinuousScheme(stripeRepairTime, mid, Pr)
		if ret1 != nil {
			stripeOrder = ret1
			minTime = ret2
			r = mid
		} else {
			l = mid
		}
	}
	return
}

func (e *Erasure) getMinimalTimeGreedy(stripeRepairTime []float64) (
	stripeOrder [][]int, minTime float64) {
	// the greedy heuristic is to prioritize the long enduring stripes, and
	// every time put the slowest in the fastest slot.
	n := len(stripeRepairTime)
	if n == 0 {
		return nil, 0
	}
	Pr := (e.MemSize * GiB) / (e.K * int(e.dataStripeSize))

	if n <= Pr {
		for i := 0; i < n; i++ {
			stripeOrder[i] = append(stripeOrder[i], i)
		}
		return stripeOrder, maxFloat64(stripeRepairTime...)
	}
	stripeOrder = make([][]int, Pr)
	h := &HeapFloat64{}
	for i := 0; i < Pr; i++ {
		stripeOrder[i] = append(stripeOrder[i], n-i-1)
		h.Push(heapv{i, stripeRepairTime[n-i-1]})
	}
	for j := n - Pr - 1; j >= 0; j-- {
		minv := heap.Pop(h).(heapv)
		stripeOrder[minv.id] = append(stripeOrder[minv.id], j)
		minTime = maxFloat64(minTime, minv.val+stripeRepairTime[j])
		heap.Push(h, heapv{minv.id, minv.val + stripeRepairTime[j]})

	}
	return
}
