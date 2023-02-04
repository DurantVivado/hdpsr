package hdpsr

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestGetMinimalTime(t *testing.T) {
	testEC := &Erasure{
		K:               6,
		M:               2,
		DiskNum:         12,
		BlockSize:       8 * MiB,
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
	err = testEC.ReadDiskInfo()
	if err != nil {
		t.Fatal(err)
	}
	// err = testEC.InitSystem(true)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	err = testEC.ReadConfig()
	if err != nil {
		t.Fatal(err)
	}
	// err = testEC.getDiskBWFIO()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	fileSize := int64(5 * GiB)
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		t.Error(err)
	}
	defer delTempDir()
	_, err := testEC.EncodeFile(inpath)
	if err != nil {
		t.Error(err)
	}
	err = testEC.WriteConfig()
	if err != nil {
		t.Error(err)
	}
	testEC.Destroy(&SimOptions{
		Mode:     "diskFail",
		FailDisk: "0",
	})
	// StripeNum := 1000
	// stripeRepairTime := []float64{2, 3, 4, 4, 4, 5, 6, 8, 8, 9}
	// stripeRepairTime := genRandArrFloat64(StripeNum, 10, 0)
	// fmt.Println("Stripe Repair Index:", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	slowLatency := 0.0
	intFi, ok := testEC.fileMap.Load(inpath)
	if !ok {
		t.Error("file not found in fileMap")
	}
	fi := intFi.(*fileInfo)

	dist := fi.Distribution
	stripeRepairTime := testEC.getStripeRepairtime(dist, slowLatency)
	fmt.Println("Stripe Repair Time :", stripeRepairTime)
	// var stripeOrder [][]int
	var minTime float64
	fmt.Printf("Continuous Method\n")
	_, minTime = testEC.getMinimalTimeContinuous(stripeRepairTime)
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

func TestFullStripeRecoverWithOrder(t *testing.T) {
	testEC := &Erasure{
		K:               4,
		M:               2,
		DiskNum:         14,
		BlockSize:       67108864,
		MemSize:         2,
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
	err = testEC.ReadDiskInfo()
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
	fileSize := int64(10 * GiB)
	fileName := fmt.Sprintf("temp-%d", fileSize)
	inpath := filepath.Join("input", fileName)
	slowLatency := 2.0
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		t.Fatal(err)
	}
	defer delTempDir()
	_, err := testEC.EncodeFile(inpath)
	if err != nil {
		t.Fatal(err)
	}
	err = testEC.WriteConfig()
	if err != nil {
		t.Fatal(err)
	}
	testEC.Destroy(&SimOptions{
		Mode:     "diskFail",
		FailDisk: "0",
	})
	schemes := []int{CONTINUOUS, GREEDY, RANDOM}
	for _, scheme := range schemes {
		start := time.Now()
		rm, err := testEC.FullStripeRecoverWithOrder(
			fileName,
			slowLatency,
			&Options{Scheme: scheme})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("Scheme %d costs: %v\n", scheme, time.Since(start))
		for old, new := range rm {
			oldPath := filepath.Join(old, fileName, "BLOB")
			newPath := filepath.Join(new, fileName, "BLOB")
			if ok, err := checkFileIfSame(newPath, oldPath); !ok && err == nil {
				t.Error(err)
			} else if err != nil {
				t.Fatal(err)
			}
		}
		if _, err := copyFile(testDiskMountPath+".old", testDiskMountPath); err != nil {
			t.Fatal(err)
		}
	}
}
