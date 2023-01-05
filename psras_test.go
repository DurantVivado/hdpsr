package hdpsr

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestPSRAS(t *testing.T) {
	testEC := &Erasure{
		K:               6,
		M:               2,
		DiskNum:         12,
		BlockSize:       512 * KiB,
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
	fileSize := int64(1 * GiB)
	fileName := fmt.Sprintf("temp-%d", fileSize)
	inpath := filepath.Join("input", fileName)
	slowLatency := 0
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		t.Fatal(err)
	}
	// defer delTempDir()
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
	start := time.Now()
	rm, err := testEC.PartialStripeRecoverPreliminary(
		fileName,
		slowLatency,
		&Options{})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("PSR-AP costs: %v\n", time.Since(start))

	for old, new := range rm {
		oldPath := filepath.Join(old, fileName, "BLOB")
		newPath := filepath.Join(new, fileName, "BLOB")
		if ok, err := checkFileIfSame(newPath, oldPath); !ok && err == nil {
			t.Error(err)
		} else if err != nil {
			t.Error(err)
		}
	}
	if _, err := copyFile(testDiskMountPath+".old", testDiskMountPath); err != nil {
		t.Error(err)
	}
}
