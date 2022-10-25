package hdpsr

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestGetMinimalTime(t *testing.T) {
	testEC := &Erasure{
		K:               6,
		M:               2,
		DiskNum:         12,
		BlockSize:       1048576,
		MemSize:         8,
		ConfigFile:      testConfigFile,
		DiskMountPath:   testDiskMountPath,
		DiskBWPath:      testDiskBWPath,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
		ReadBWfromFile:  true,
	}
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
	// err = testEC.getDiskBWFIO()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	fileSize := int64(1 * GiB)
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		t.Error(err)
	}
	// defer deleteTempFiles([]int64{fileSize})
	_, err := testEC.EncodeFile(inpath)
	if err != nil {
		t.Error(err)
	}
	err = testEC.WriteConfig()
	if err != nil {
		t.Error(err)
	}

	// StripeNum := 1000
	// stripeRepairTime := []float64{2, 3, 4, 4, 4, 5, 6, 8, 8, 9}
	// stripeRepairTime := genRandArrFloat64(StripeNum, 10, 0)
	// fmt.Println("Stripe Repair Index:", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	slowLatency := 4
	stripeRepairTime := testEC.getStripeRepairtime(slowLatency)
	fmt.Println("Stripe Repair Time :", stripeRepairTime)
	// var stripeOrder [][]int
	var minTime float64
	fmt.Printf("Continuous Method\n")
	_, minTime = testEC.getMinimalTimeContinous(stripeRepairTime)
	fmt.Printf("minTime:%f\n", minTime)
	// fmt.Printf("stripeOrder:%v\n", stripeOrder)

	fmt.Printf("Non-Continuous Greedy Method\n")
	_, minTime = testEC.getMinimalTimeGreedy(stripeRepairTime)
	fmt.Printf("minTime:%f\n", minTime)
	// fmt.Printf("stripeOrder:%v\n", stripeOrder)

	fmt.Printf("Non-Continuous Random Method\n")
	_, minTime = testEC.getMinimalTimeRand(stripeRepairTime)
	fmt.Printf("minTime:%f\n", minTime)
	// fmt.Printf("stripeOrder:%v\n", stripeOrder)
}
