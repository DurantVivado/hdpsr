package hdpsr

import (
	"fmt"
	"testing"
)

func TestGetMinimalTime(t *testing.T) {
	testEC := &Erasure{
		K:               6,
		M:               2,
		DiskNum:         12,
		BlockSize:       67108864,
		MemSize:         8,
		ConfigFile:      "conf.json",
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	//1. read disk paths
	err = testEC.ReadDiskPath()
	if err != nil {
		t.Fatal(err)
	}
	err = testEC.InitSystem(true)
	if err != nil {
		t.Fatal(err)
	}
	err = testEC.ReadConfig()
	if err != nil {
		t.Fatal(err)
	}
	// StripeNum := 10
	stripeRepairTime := []float64{2, 3, 4, 4, 4, 5, 6, 8, 8, 9}
	// genRandArrFloat64(StripeNum, 4, 0)

	fmt.Println("Stripe Repair Time:", stripeRepairTime)
	var stripeOrder [][]int
	var minTime float64
	fmt.Printf("Continuous Method\n")
	stripeOrder, minTime = testEC.getMinimalTime(stripeRepairTime)
	fmt.Printf("minTime:%f\n", minTime)
	fmt.Printf("stripeOrder:%v\n", stripeOrder)

	fmt.Printf("Non-Continuous Greedy Method\n")
	stripeOrder, minTime = testEC.getMinimalTimeGreedy(stripeRepairTime)
	fmt.Printf("minTime:%f\n", minTime)
	fmt.Printf("stripeOrder:%v\n", stripeOrder)

}
