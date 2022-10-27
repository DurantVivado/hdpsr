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
