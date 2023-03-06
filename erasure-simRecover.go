package hdpsr

func (e *Erasure) LB_HDR_SIM(stripeNum int, failDiskSet []int, seed int64) {
	dist := e.genStripeDist(stripeNum, seed)
	replaceMap := make(map[int]int, 0)
	i := 0
	for disk := range failDiskSet {
		replaceMap[disk] = e.DiskNum + i
		i++
	}
	_ = e.findFirstKScheme(dist, replaceMap)
	_ = e.findBalanceScheme(dist, replaceMap)
	_ = e.findRandomScheme(dist, replaceMap)
}
