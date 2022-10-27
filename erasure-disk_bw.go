package hdpsr

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"
)

// getDiskBWRead reads a fix-sized chunk and get the disk bandwidth
const (
	CHUNKSIZE  = 50 * KiB
	FIORUNTIME = 60
	IODEPTH    = 16
)

func (e *Erasure) getDiskBWRead(ifs []*os.File) error {
	erg := new(errgroup.Group)
	for i, disk := range e.diskInfos[0:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			if !disk.available {
				return nil
			}
			buf := make([]byte, CHUNKSIZE)
			start := time.Now()
			_, err = ifs[i].Read(buf)
			if err != nil && err != io.EOF {
				return err
			}
			disk.read_bw =
				float64(CHUNKSIZE/KiB) / time.Since(start).Seconds()
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		return err
	}
	if !e.Quiet {
		for _, disk := range e.diskInfos[0:e.DiskNum] {
			log.Printf("%s bandwidth %.3f Byte/s\n",
				disk.mntPath, disk.read_bw)
		}
	}
	return nil
}

// fioGetDiskBW proactively measures disk bandwidth using `fio`
func FIO(dev string, bs string, rw string, ioengine string, iodepth int, runtime int) (
	string, error) {
	name := fmt.Sprintf("Job-%s-%s-%s", dev, bs, rw)
	cmd := fmt.Sprintf("fio -filename=%s -bs=%s -direct=1 -thread -rw=%s -name=%s -ioengine=%s -iodepth=%d -runtime=%d",
		dev, bs, rw, name, ioengine, iodepth, runtime)
	return execShell(cmd)
}

func getBWReadFIO(blocksize string, dev string) (float64, error) {
	name := fmt.Sprintf("Job-%s-%s-%s", dev, blocksize, "read")
	subcmd := "| grep BW | awk -F ' ' '{print $(NF-1)}' | tr -d 'BW=Mi/s'"
	cmd := fmt.Sprintf("fio -filename=%s -bs=%s -direct=1 -thread -rw=%s -name=%s -ioengine=%s -iodepth=%d -runtime=%d %s",
		dev, blocksize, "read", name, "libaio", IODEPTH, FIORUNTIME, subcmd)
	result, err := execShell(cmd)
	if err != nil {
		return 0, err
	}
	ret, _ := strconv.ParseFloat(result, 64)
	return ret, nil
}

func getBWRandReadFIO(blocksize string, dev string) (float64, error) {
	name := fmt.Sprintf("Job-%s-%s-%s", dev, blocksize, "read")
	subcmd := "| grep BW | awk -F ' ' '{print $(NF-1)}' | tr -d 'BW=Mi/s'"
	cmd := fmt.Sprintf("fio -filename=%s -bs=%s -direct=1 -thread -rw=%s -name=%s -ioengine=%s -iodepth=%d -runtime=%d %s",
		dev, blocksize, "randread", name, "libaio", IODEPTH, FIORUNTIME, subcmd)
	result, err := execShell(cmd)
	if err != nil {
		return 0, err
	}
	ret, _ := strconv.ParseFloat(result, 64)
	return ret, nil
}

// Use below commands will implicitly ruin the filesystem
func getBWWriteFIO(blocksize string, dev string) (float64, error) {
	name := fmt.Sprintf("Job-%s-%s-%s", dev, blocksize, "read")
	subcmd := "| grep BW | awk -F ' ' '{print $3}' | tr -d 'BW=Mi/s'"
	cmd := fmt.Sprintf("fio -filename=%s -bs=%s -direct=1 -thread -rw=%s -name=%s -ioengine=%s -iodepth=%d -runtime=%d -allow_mounted_write=1 %s",
		dev, blocksize, "write", name, "libaio", IODEPTH, FIORUNTIME, subcmd)
	result, err := execShell(cmd)
	if err != nil {
		return 0, err
	}
	ret, _ := strconv.ParseFloat(result, 64)
	return ret, nil
}

// Use below commands will implicitly ruin the filesystem, use with care
func getBWRandWriteFIO(blocksize string, dev string) (float64, error) {
	name := fmt.Sprintf("Job-%s-%s-%s", dev, blocksize, "read")
	subcmd := "| grep BW | awk -F ' ' '{print $3}' | tr -d 'BW=Mi/s'"
	cmd := fmt.Sprintf("fio -filename=%s -bs=%s -direct=1 -thread -rw=%s -name=%s -ioengine=%s -iodepth=%d -runtime=%d -allow_mounted_write=1 %s",
		dev, blocksize, "randwrite", name, "libaio", IODEPTH, FIORUNTIME, subcmd)
	result, err := execShell(cmd)
	if err != nil {
		return 0, err
	}
	ret, _ := strconv.ParseFloat(result, 64)
	return ret, nil
}

func getBlockDevice(mountPath string) (string, error) {
	cmd := fmt.Sprintf("df | grep -w %s | awk -F ' ' '{print $1}'", mountPath)
	result, err := execShell(cmd)
	if err != nil {
		return "", err
	}
	return result, nil
}

func (e *Erasure) getDiskBWFIO() error {
	erg := new(errgroup.Group)
	if !e.Quiet {
		log.Println("get Disk BW...")
	}
	for _, disk := range e.diskInfos {
		// find the corresponding block device of mntPath
		disk := disk
		erg.Go(func() error {
			blkdev, err := getBlockDevice(disk.mntPath)
			if err != nil {
				return err
			}
			readbw, err := getBWReadFIO(fmt.Sprint(e.BlockSize), blkdev)
			if err != nil {
				return err
			}
			disk.read_bw = readbw
			randreadbw, err := getBWRandReadFIO(fmt.Sprint(e.BlockSize), blkdev)
			if err != nil {
				return err
			}
			disk.randread_bw = randreadbw
			// warning: the disk will be write to full
			// writebw, err := getBWWriteFIO(fmt.Sprint(e.BlockSize), blkdev)
			// if err != nil {
			// 	return err
			// }
			// disk.write_bw = writebw
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		return err
	}
	f, err := os.OpenFile(e.DiskBWPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := bufio.NewWriter(f)
	for _, disk := range e.diskInfos {
		data := fmt.Sprintf("%.3f %.3f %.3f %.3f\n",
			disk.read_bw,
			disk.write_bw,
			disk.randread_bw,
			disk.randwrite_bw)
		_, err = buf.Write([]byte(data))
		if err != nil {
			return err
		}
		buf.Flush()
	}
	return nil
}
