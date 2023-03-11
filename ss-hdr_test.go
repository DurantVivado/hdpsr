package hdpsr

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStripeSchedule(t *testing.T) {
	seed := int64(1000000009)
	stripeNum := 10000
	// write to an csv file
	filename := fmt.Sprintf("SS_HDR_SIM_%v.csv", time.Now().Unix())
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		t.Fatal("csv file failed to open")
	}
	defer f.Close()
	w := csv.NewWriter(f)
	DN := &[]int{20, 40, 80, 160}
	KMpairs := &[]KMpair{
		KMpair{4, 2}, KMpair{6, 3}, KMpair{8, 4}, KMpair{12, 4},
	}
	for _, dn := range *DN {
		w.Write([]string{"DiskNum:", fmt.Sprintf("%d", dn)})
		for _, p := range *KMpairs {
			fmt.Printf("dn:%d, k:%d, m:%d\n", dn, p.K, p.M)
			w.Write([]string{"DataShards:", fmt.Sprintf("%d", p.K), "ParityShards", fmt.Sprintf("%d", p.M)})
			e := &Erasure{
				K:               p.K,
				M:               p.M,
				DiskNum:         dn,
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
			e.dataStripeSize = int64(e.K) * e.BlockSize
			e.allStripeSize = int64(e.K+e.M) * e.BlockSize
			failDiskSet := &[]int{0}
			dist, blk2off := e.genStripeDist(stripeNum, seed)
			fi := &fileInfo{
				Distribution:  dist,
				blockToOffset: blk2off,
			}
			replaceMap := make(map[int]int, 0)
			i := 0
			for disk := range *failDiskSet {
				replaceMap[disk] = e.DiskNum + i
				i++
			}
			var minTimeSlice int
			w.Write([]string{"Scheme", "avgMinTimeSlice"})
			scheme, _ := e.findFirstKScheme(fi, replaceMap)
			// iterate ten times and average the outcome
			avgMinT_SEQ, avgMinT_SS_HDR := float64(0), float64(0)
			for i := 0; i < 10; i++ {
				_, minTimeSlice = e.getMinimalTimeSequence(scheme, replaceMap)
				avgMinT_SEQ += float64(minTimeSlice)
				_, minTimeSlice = e.getMinimalTimeStripeScheduled(scheme, replaceMap)
				avgMinT_SS_HDR += float64(minTimeSlice)
			}
			avgMinT_SEQ /= 10
			avgMinT_SS_HDR /= 10

			w.Write([]string{"SEQUENCE", fmt.Sprintf("%.3f", avgMinT_SEQ)})
			w.Write([]string{"SS_HDR", fmt.Sprintf("%.3f", avgMinT_SS_HDR)})
		}
		w.Write([]string{"\n"})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal("error writing csv:", err)
	}
}

func TestStripeScheduleRecover(t *testing.T) {
	testEC := &Erasure{
		K:               4,
		M:               2,
		DiskNum:         16,
		BlockSize:       64 * MiB,
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
	err = testEC.ReadConfig()
	if err != nil {
		t.Fatal(err)
	}
	testEC.Destroy(&SimOptions{
		Mode:     "diskFail",
		FailDisk: "0",
	})
	fileSize := int64(5 * GiB)
	fileName := fmt.Sprintf("temp-%d", fileSize)
	method1 := []string{"FIRST_K", "LB_HDR", "RANDOM_K"}
	method2 := []string{"SEQ", "SS_HDR"}
	for _, m1 := range method1 {
		for _, m2 := range method2 {
			start := time.Now()
			rm, err := testEC.StripeScheduleRecover(
				fileName,
				&Options{Method1: m1, Method2: m2, WriteToBackup: true})
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("[%s, %s] costs: %v\n", m1, m2, time.Since(start))
			for old, new := range rm {
				oldPath := filepath.Join(old, fileName, "BLOB")
				newPath := filepath.Join(new, fileName, "BLOB")
				if ok, err := checkFileIfSame(newPath, oldPath); !ok && err == nil {
					t.Fatal(err)
				} else if err != nil {
					t.Fatal(err)
				}
			}
			// if _, err := copyFile(testDiskMountPath+".old", testDiskMountPath); err != nil {
			// 	t.Fatal(err)
			// }
		}
	}
}
