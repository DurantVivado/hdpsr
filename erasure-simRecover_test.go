package hdpsr

import (
	"encoding/csv"
	"fmt"
	"os"
	"testing"
	"time"
)

type KMpair struct {
	K int
	M int
}

func TestLB_HDR_SIM(t *testing.T) {
	seed := int64(1000000007)
	stripeNum := 10000
	// write to an csv file
	filename := fmt.Sprintf("LB_HDR_SIM_%v.csv", time.Now().Unix())
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
			failDiskSet := &[]int{0}
			dist := e.genStripeDist(stripeNum, seed)
			fi := &fileInfo{
				Distribution: dist,
			}
			replaceMap := make(map[int]int, 0)
			i := 0
			for disk := range *failDiskSet {
				replaceMap[disk] = e.DiskNum + i
				i++
			}
			var sumDisk []int
			w.Write([]string{"Scheme", "avgMaxLoad", "avgLoadSD"})

			// iterate ten times and average the outcome
			avgMaxLoad_FK, avgMaxLoad_RK, avgMaxLoad_BK := float64(0), float64(0), float64(0)
			avgSDLoad_FK, avgSDLoad_RK, avgSDLoad_BK := float64(0), float64(0), float64(0)
			for i := 0; i < 10; i++ {
				_, sumDisk = e.findFirstKScheme(fi, replaceMap)
				avgMaxLoad_FK += float64(maxInts(sumDisk[1:]))
				avgSDLoad_FK += calcSDInt(sumDisk[1:])
				_, sumDisk = e.findRandomScheme(fi, replaceMap)
				avgMaxLoad_RK += float64(maxInts(sumDisk[1:]))
				avgSDLoad_RK += calcSDInt(sumDisk[1:])
				_, sumDisk = e.findBalanceScheme(fi, replaceMap)
				avgMaxLoad_BK += float64(maxInts(sumDisk[1:]))
				avgSDLoad_BK += calcSDInt(sumDisk[1:])
			}
			avgMaxLoad_FK /= 10
			avgSDLoad_FK /= 10
			avgMaxLoad_RK /= 10
			avgSDLoad_RK /= 10
			avgMaxLoad_BK /= 10
			avgSDLoad_BK /= 10

			w.Write([]string{"FirstK", fmt.Sprintf("%.3f", avgMaxLoad_FK), fmt.Sprintf("%.3f", avgSDLoad_FK)})
			w.Write([]string{"RandomK", fmt.Sprintf("%.3f", avgMaxLoad_RK), fmt.Sprintf("%.3f", avgSDLoad_RK)})
			w.Write([]string{"BalanceK", fmt.Sprintf("%.3f", avgMaxLoad_BK), fmt.Sprintf("%.3f", avgSDLoad_BK)})
		}
		w.Write([]string{"\n"})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal("error writing csv:", err)
	}
}
