package hdpsr

import "testing"

func TestLB_HDR_SIM(t *testing.T) {
	seed := int64(1000000007)
	stripeNum := 10000
	e := &Erasure{
		K:               6,
		M:               2,
		DiskNum:         200,
		BlockSize:       8 * MiB,
		MemSize:         8,
		ConfigFile:      testConfigFile,
		DiskMountPath:   testDiskMountPath,
		DiskBWPath:      testDiskBWPath,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           false,
		ReadBWfromFile:  true,
	}
	failDiskArr := &[]int{0}
	e.LB_HDR_SIM(stripeNum, *failDiskArr, seed)
}
