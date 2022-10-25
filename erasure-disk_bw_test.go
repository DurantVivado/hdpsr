package hdpsr

import (
	"fmt"
	"testing"
)

func TestRunFIO(t *testing.T) {
	ret, err := FIO("/dev/vdb", "64M", "read", "libaio", 16, 15)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(ret))
}

func TestGetBWReadFIO(t *testing.T) {
	ret, err := getBWReadFIO("4k", "/dev/vdb")
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("Read BW: %f MiB/s\n", ret)
}

func TestGetBWRandReadFIO(t *testing.T) {
	ret, err := getBWRandReadFIO("4k", "/dev/vdb")
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("RandRead BW: %f MiB/s\n", ret)
}

func TestGetBWWRandriteFIO(t *testing.T) {
	ret, err := getBWRandWriteFIO("4k", "/dev/vdb")
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("RandWrite BW: %f MiB/s\n", ret)
}

func TestGetBWWriteFIO(t *testing.T) {
	ret, err := getBWWriteFIO("4k", "/dev/vdb")
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("Write BW: %f MiB/s\n", ret)
}

func TestGetDiskBWFIO(t *testing.T) {
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
	err := testEC.getDiskBWFIO()
	if err != nil {
		t.Fatal(err)
	}
	for _, disk := range testEC.diskInfos {
		fmt.Printf("%s Read BW : %.3f MiB/s; Write BW : %.3f MiB/s\n",
			disk.mntPath, disk.read_bw, disk.write_bw)
	}
}
