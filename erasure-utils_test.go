package hdpsr

import (
	"fmt"
	"testing"
)

func TestExecShell(t *testing.T) {
	ret, err := execShell("df -hT")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(ret))

}

func TestGetBlockDevice(t *testing.T) {
	blkdev, err := getBlockDevice("/mnt/disk1")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(blkdev)
}

func TestGetDiskUsage(t *testing.T) {
	size, used, avail := getDiskUsage("/dev/vdb1", 4096)
	fmt.Printf("size:%d used:%d avail:%d\n", size, used, avail)
}

func TestGetMemoryUsage(t *testing.T) {
	total, used, free := getMemUsage()
	fmt.Printf("total:%d Gi, used:%d Gi, free:%d Gi\n", total, used, free)

}
