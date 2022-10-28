package hdpsr

import "testing"

func TestReset(t *testing.T) {
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
	err = testEC.reset()
	if err != nil {
		t.Fatal(err)
	}
}
