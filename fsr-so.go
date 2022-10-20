package hdpsr

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

func findNonContinuousGreedy(A []float64, mid float64, scheme *[][]int, minTime *float64) bool {
	group := make([]int, 0)
	return helper(A, mid, 0, 0, group, scheme, minTime)
}

func findContinousScheme(A []float64, mid float64, Pr int, scheme *[][]int, minTime *float64) bool {
	cnt := 1
	sum := A[0]
	maxSubTime := A[0]
	for i := 1; i < len(A); i++ {
		if sum+A[i] > mid {
			cnt++
			*minTime += maxSubTime
			sum = 0
		}
		sum += A[i]
		(*scheme)[cnt-1] = append((*scheme)[cnt-1], i)
		maxSubTime = maxFloat64(maxSubTime, A[i])
	}
	return cnt <= Pr
}

// full-stripe-repair with stripe order first
func (e *Erasure) getMinimalTime(stripeRepairTime []float64) (
	stripeOrder [][]int, minTime float64) {
	if len(stripeRepairTime) == 0 {
		return nil, 0
	}
	Pr := (e.MemSize * GiB) / (e.K * int(e.dataStripeSize))

	if len(stripeRepairTime) <= Pr {
		for i := 0; i < len(stripeRepairTime); i++ {
			stripeOrder[i] = append(stripeOrder[i], i)
		}
		return stripeOrder, maxFloat64(stripeRepairTime...)
	}
	maxTime := maxFloat64(stripeRepairTime...)
	sumTime := sumFloat64(stripeRepairTime...)
	l, r := maxTime, sumTime

	for l <= r {
		mid := l + (r-l)/2
		stripeOrder = make([][]int, Pr)
		minTime = float64(0)
		if findContinousScheme(stripeRepairTime, mid, Pr, &stripeOrder, &minTime) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	return
}
