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
			dist := e.genStripeDist(stripeNum, seed)
			replaceMap := make(map[int]int, 0)
			i := 0
			for disk := range *failDiskSet {
				replaceMap[disk] = e.DiskNum + i
				i++
			}
			var minTimeSlice int
			w.Write([]string{"Scheme", "avgMinTimeSlice"})

			// iterate ten times and average the outcome
			avgMinT_SEQ, avgMinT_SS_HDR := float64(0), float64(0)
			for i := 0; i < 10; i++ {
				_, minTimeSlice = e.getMinimalTimeSequence(dist, replaceMap)
				avgMinT_SEQ += float64(minTimeSlice)
				_, minTimeSlice = e.getMinimalTimeStripeScheduled(dist, replaceMap)
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

func TestSS_HDR(t *testing.T) {
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
	fileSize := int64(1 * GiB)
	fileName := fmt.Sprintf("temp-%d", fileSize)
	inpath := filepath.Join("input", fileName)
	// slowLatency := 0.0
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		t.Fatal(err)
	}
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
	schemes := []int{SEQUENCE, SS_HDR}
	for _, scheme := range schemes {
		start := time.Now()
		rm, err := testEC.SS_HDR(
			fileName,
			&Options{Scheme: scheme})
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
