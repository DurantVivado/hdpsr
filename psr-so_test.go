package hdpsr

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestPartialStripeRecoverWithOrder(t *testing.T) {
	testEC := &Erasure{
		K:               4,
		M:               2,
		DiskNum:         14,
		BlockSize:       64 * MiB,
		MemSize:         2,
		ConfigFile:      "examples/conf.json",
		DiskMountPath:   "examples/.hdr.disks.path",
		DiskBWPath:      "examples/diskBW",
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
	// fileSize := int64(1 * GiB)
	fileName := "test-64Mx160"
	// fileName := fmt.Sprintf("temp-%d", fileSize)
	// inpath := filepath.Join("input", fileName)
	slowLatency := 2.0
	// err = generateRandomFileBySize(inpath, fileSize)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer delTempDir()
	// _, err := testEC.EncodeFile(inpath)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// err = testEC.WriteConfig()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	testEC.Destroy(&SimOptions{
		Mode:     "diskFail",
		FailDisk: "0",
	})
	intraStripe := testEC.getIntraStripeOptimal(slowLatency)
	fmt.Println("intraStripe:", intraStripe)

	schemes := []int{GREEDY, CONTINUOUS}
	for _, scheme := range schemes {
		start := time.Now()
		rm, err := testEC.PartialStripeRecoverWithOrder(
			fileName,
			slowLatency,
			&Options{
				Scheme:      scheme,
				intraStripe: intraStripe,
			})
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

func TestPartialStripeRecoverWithOrderII(t *testing.T) {
	testEC := &Erasure{
		K:               10,
		M:               4,
		DiskNum:         14,
		BlockSize:       64 * MiB,
		MemSize:         2,
		ConfigFile:      "examples/conf.json",
		DiskMountPath:   "examples/.hdr.disks.path",
		DiskBWPath:      "examples/diskBW",
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
	// fileSize := int64(1 * GiB)
	filePath := "/mnt/disk16/test-64Mx160"
	fileName := filepath.Base(filePath)
	// fileName := fmt.Sprintf("temp-%d", fileSize)
	// inpath := filepath.Join("input", fileName)
	slowLatency := 0.5
	// err = generateRandomFileBySize(inpath, fileSize)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer delTempDir()
	// _, err := testEC.EncodeFile(filePath)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// err = testEC.WriteConfig()
	// if err != nil {
	// 	t.Fatal(err)
	// }
	testEC.PrintDiskInfo()
	fmt.Println("optimal intrastripe:", testEC.getIntraStripeOptimal(slowLatency))
	testEC.Destroy(&SimOptions{
		Mode:     "diskFail",
		FailDisk: "0",
	})

	scheme := GREEDY
	intraStripes := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, intraStripe := range intraStripes {
		start := time.Now()
		rm, err := testEC.PartialStripeRecoverWithOrder(
			fileName,
			slowLatency,
			&Options{
				Scheme:      scheme,
				intraStripe: intraStripe,
			})
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("IntraStripe %d costs: %v\n", intraStripe, time.Since(start))
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
