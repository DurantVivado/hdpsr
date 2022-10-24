package hdpsr

import (
	"fmt"
	"testing"
)

func TestExecShell(t *testing.T) {
	ret, err := execShell("fio -filename=/dev/vdb -bs=4k -direct=1 -thread -rw=read -ioengine=libaio -name=/dev/vdb -iodepth=16 -runtime=1 | grep BW | awk -F ' ' '{print $(NF-1)}' | cut -c 4-7")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(ret))

}

func TestRunFIO(t *testing.T) {
	ret, err := FIO("/dev/vdb", "64M", "read", "libaio", 16, 15)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(ret))
}

func TestGrepDiskBWRead(t *testing.T) {
	ret, err := grepBWRead("4k", "/dev/vdb")
	if err != nil {
		t.Error(err)
	}
	fmt.Println("Read BW:", ret)
}
