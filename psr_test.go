package hdpsr

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestPartialStripeRecover(t *testing.T) {
	t.Helper()
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
		Quiet:           false,
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
	fileSize := int64(1 * GiB)
	fileName := fmt.Sprintf("temp-%d", fileSize)
	inpath := filepath.Join("input", fileName)
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		t.Fatal(err)
	}
	// defer delTempDir()
	// encode the file for first use
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
	start := time.Now()
	rm, err := testEC.PartialStripeRecover(
		fileName,
		&Options{},
	)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("PSR costs: %v\n", time.Since(start))
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
		t.Error(err)
	}

}
