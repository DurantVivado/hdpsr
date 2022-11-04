package hdpsr

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const (
	FIRST_K = iota
	FASTEST_K
	RANDOM_K
	BALANCE_K
)

func (e *Erasure) diskMetric(load, disk_id int) float64 {
	return float64(load) / e.diskInfos[disk_id].read_bw
}

// the default scheme: for each stripe, read the first k blocks
func (e *Erasure) findFirstKScheme(dist [][]int, replaceMap map[int]int) (
	firstKScheme [][]int) {
	stripeNum := len(dist)
	failStripeSet := &IntSet{}
	firstKScheme = make([][]int, stripeNum)
	maxLoad := 0
	sumDisk := make([]int, e.DiskNum)
	sumLoad := 0
	for s := 0; s < stripeNum; s++ {
		for i := 0; i < e.K+e.M; i++ {
			if _, ok := replaceMap[dist[s][i]]; ok {
				failStripeSet.Insert(s)
				break
			}
		}
	}
	for s := 0; s < stripeNum; s++ {
		if failStripeSet.Exist(s) {
			for i := 0; i < e.K+e.M; i++ {
				diskId := dist[s][i]
				if _, ok := replaceMap[diskId]; !ok {
					firstKScheme[s] = append(firstKScheme[s], diskId)
					sumDisk[diskId]++
					maxLoad = maxInt(maxLoad, sumDisk[diskId])
					sumLoad++
					if len(firstKScheme[s]) == e.K {
						break
					}
				}
			}
		} else {
			firstKScheme[s] = dist[s]
		}
	}
	if !e.Quiet {
		fmt.Printf("---------------FSR-B_1K Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	return
}

// the default scheme: for each stripe, read the fastest k blocks,
// the corresponding disk of which has the largest bandwidth
func (e *Erasure) findFastestKScheme(dist [][]int, replaceMap map[int]int) (
	fastestKScheme [][]int) {
	stripeNum := len(dist)
	failStripeSet := &IntSet{}
	fastestKScheme = make([][]int, stripeNum)
	maxLoad := 0
	sumDisk := make([]int, e.DiskNum)
	sumLoad := 0
	for s := 0; s < stripeNum; s++ {
		for i := 0; i < e.K+e.M; i++ {
			if _, ok := replaceMap[dist[s][i]]; ok {
				failStripeSet.Insert(s)
				break
			}
		}
	}
	for s := 0; s < stripeNum; s++ {
		if failStripeSet.Exist(s) {
			diskVec := make([]int, 0)
			for i := 0; i < e.K+e.M; i++ {
				diskId := dist[s][i]
				if _, ok := replaceMap[diskId]; !ok {
					diskVec = append(diskVec, diskId)
				}
			}
			sort.Slice(diskVec, func(i, j int) bool {
				return e.diskInfos[i].read_bw > e.diskInfos[j].read_bw
			})
			for j := 0; j < e.K; j++ {
				sumDisk[diskVec[j]]++
				sumLoad++
				maxLoad = maxInt(maxLoad, sumDisk[diskVec[j]])
			}
			fastestKScheme[s] = diskVec[:e.K]
		} else {
			fastestKScheme[s] = dist[s]
		}
	}
	if !e.Quiet {
		fmt.Printf("---------------FSR-B_FK Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	return
}

// for each failed stripe, randomly pick up k blocks
func (e *Erasure) findRandomScheme(dist [][]int, replaceMap map[int]int) (
	randomScheme [][]int) {
	stripeNum := len(dist)
	failStripeSet := &IntSet{}
	randomScheme = make([][]int, stripeNum)
	maxLoad := 0
	sumDisk := make([]int, e.DiskNum)
	sumLoad := 0
	for s := 0; s < stripeNum; s++ {
		for i := 0; i < e.K+e.M; i++ {
			if _, ok := replaceMap[dist[s][i]]; ok {
				failStripeSet.Insert(s)
				break
			}
		}
	}
	for s := 0; s < stripeNum; s++ {
		if failStripeSet.Exist(s) {
			diskVec := make([]int, 0)
			for i := 0; i < e.K+e.M; i++ {
				diskId := dist[s][i]
				if _, ok := replaceMap[diskId]; !ok {
					diskVec = append(diskVec, diskId)
				}
			}
			rand.Shuffle(len(diskVec), func(i, j int) {
				diskVec[i], diskVec[j] = diskVec[j], diskVec[i]
			})
			for j := 0; j < e.K; j++ {
				sumDisk[diskVec[j]]++
				sumLoad++
				maxLoad = maxInt(maxLoad, sumDisk[diskVec[j]])
			}
			randomScheme[s] = diskVec[:e.K]
		} else {
			randomScheme[s] = dist[s]
		}
	}
	if !e.Quiet {
		fmt.Printf("---------------FSR-B_R Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	return
}

func (e *Erasure) findBalanceScheme(dist [][]int, replaceMap map[int]int) (
	balanceScheme [][]int) {
	stripeNum := len(dist)
	maxLoad := 0
	sumLoad := 0
	failStripeNum := 0
	failStripeSet := &IntSet{}
	balanceScheme = make([][]int, stripeNum)
	sumDisk := make([]int, e.DiskNum)
	stripeRedu := make(map[int]int)
	diskLoads := make([]int, e.DiskNum)
	diskDict := make([]IntSet, e.DiskNum)
	available := 0
	for s := 0; s < stripeNum; s++ {
		flag := true
		failBlk := 0
		for i := 0; i < e.K+e.M; i++ {
			if _, ok := replaceMap[dist[s][i]]; ok {
				if _, ok := stripeRedu[s]; ok {
					stripeRedu[s]--
				} else {
					stripeRedu[s] = e.M - 1
				}
				failBlk++
				flag = false
			} else {
				diskLoads[dist[s][i]]++
				diskDict[dist[s][i]].Insert(s)
			}

		}
		if failBlk > 0 {
			available += (e.M - failBlk)
		}
		if !flag {
			failStripeNum += 1
			failStripeSet.Insert(s)
		}
	}
	failStripeVec := []int{}
	for failStripe := range *failStripeSet {
		failStripeVec = append(failStripeVec, failStripe)
	}
	maxload_idx := e.DiskNum - 1
	failReduList := &IntSet{}
	maxReduVec := &IntSet{}
	last_available := 0
	for available > 0 {
		//we obtain current load set for each disk and sort in descending order
		tempDiskLoad := make([]int, e.DiskNum)
		copy(tempDiskLoad, diskLoads)
		sort.Slice(tempDiskLoad, func(i, j int) bool {
			return e.diskMetric(tempDiskLoad[i], i) <
				e.diskMetric(tempDiskLoad[j], j)
		})
		curMaxLoad := tempDiskLoad[maxload_idx]
		maxRedu := 0
		maxReduVec.Clear()
		for d := 0; d < e.DiskNum; d++ {
			if _, ok := replaceMap[d]; !ok && !failReduList.Exist(d) &&
				diskLoads[d] == curMaxLoad {
				reduNum := len(diskDict[d])
				if reduNum > maxRedu {
					maxReduVec.Clear()
					maxReduVec.Insert(d)
					maxRedu = reduNum
				} else if reduNum == maxRedu {
					maxReduVec.Insert(d)
				}
			}

		}
		if maxReduVec.Empty() {
			maxload_idx--
			continue
		}
		//if current maximally loaded disk are fully reduced
		//we don't have to judge whether the current maxmimal load is accessible
		isMaxReducible := false
		for j := range *maxReduVec {
			for k := range diskDict[j] {
				if stripeRedu[k] > 0 {
					available--
					stripeRedu[k]--
					diskDict[j].Erase(k)
					diskLoads[j]--
					isMaxReducible = true
					break
				}
				if isMaxReducible {
					break
				}
			}
		}
		//if current maximally loaded disk are fully reduced
		//He could borrow some money from previously the richest relatives, for illustration
		if !isMaxReducible {
			for j := range *maxReduVec {
				for s := range *failStripeSet {
					for n := 0; n < e.DiskNum; n++ {
						if _, ok := replaceMap[n]; !ok &&
							!failReduList.Exist(n) &&
							!diskDict[j].Exist(s) &&
							diskDict[n].Exist(s) &&
							diskLoads[n] == diskLoads[j]-1 {

							diskDict[n].Insert(s)
							diskDict[j].Erase(s)
							diskLoads[n]++
							diskLoads[j]--
							isMaxReducible = true
							break
						}

					}
					if isMaxReducible {
						break
					}
				}
				if available == last_available {
					failReduList.Insert(j)
				}
				if isMaxReducible {
					break
				}
			}
		}
		last_available = available
	}

	for s := 0; s < stripeNum; s++ {
		if failStripeSet.Exist(s) {
			for i := 0; i < e.K+e.M; i++ {
				diskId := dist[s][i]
				if _, ok := replaceMap[diskId]; !ok && diskDict[diskId].Exist(s) {

					balanceScheme[s] = append(balanceScheme[s], diskId)
					sumDisk[diskId]++
					maxLoad = maxInt(maxLoad, sumDisk[diskId])
					sumLoad++
				}
			}
		} else {
			balanceScheme[s] = dist[s]
		}
	}
	if !e.Quiet {
		fmt.Printf("---------------FSR-B_B Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	return
}

// the idea is to select k blocks from the stripe
func (e *Erasure) FullStripeRecoverBlockSelected(fileName string, options *Options) (
	map[string]string, error) {
	baseFileName := filepath.Base(fileName)
	ReplaceMap := make(map[string]string)
	replaceMap := make(map[int]int)
	intFi, ok := e.fileMap.Load(baseFileName)
	if !ok {
		return nil, errFileNotFound
	}
	fi := intFi.(*fileInfo)

	fileSize := fi.FileSize
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	dist := fi.Distribution
	//first we check the number of alive disks
	// to judge if any part need reconstruction
	alive := int32(0)
	failNum := int32(0)
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)

	for i, disk := range e.diskInfos[:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.mntPath, baseFileName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				atomic.AddInt32(&failNum, 1)
				return &diskError{disk.mntPath, " available flag set false"}
			}
			ifs[i], err = os.Open(blobPath)
			if err != nil {
				disk.available = false
				return err
			}

			disk.available = true
			atomic.AddInt32(&alive, 1)
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("%s", err.Error())
		}
	}
	j := e.DiskNum
	// think what if backup also breaks down, future stuff
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			ReplaceMap[e.diskInfos[i].mntPath] = e.diskInfos[j].mntPath
			replaceMap[i] = j
			j++
		}
	}
	defer func() {
		for i := 0; i < e.DiskNum; i++ {
			if ifs[i] != nil {
				ifs[i].Close()
			}
		}
	}()
	if int(alive) < e.K {
		//the disk renders inrecoverable
		return nil, errTooFewDisksAlive
	}

	//the failure number doesn't exceed the fault tolerance
	//but unluckily we don't have enough backups!
	if int(failNum) > len(e.diskInfos)-e.DiskNum {
		return nil, errNotEnoughBackupForRecovery
	}
	rfs := make([]*os.File, failNum)
	//open restore path IOs
	for i, disk := range e.diskInfos[e.DiskNum : e.DiskNum+int(failNum)] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.mntPath, baseFileName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if e.Override {
				if err := os.RemoveAll(folderPath); err != nil {
					return err
				}
			}
			if err := os.Mkdir(folderPath, 0666); err != nil {
				return errDataDirExist
			}
			rfs[i], err = os.OpenFile(blobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
			if err != nil {
				return err
			}

			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("create BLOB failed %s", err.Error())
		}
	}
	defer func() {
		for i := 0; i < int(failNum); i++ {
			if rfs[i] != nil {
				rfs[i].Close()
			}
		}
	}()
	if int(alive) == e.DiskNum {
		if !e.Quiet {
			log.Println("start reading blocks")
		}
	} else {
		if !e.Quiet {
			log.Println("start reconstructing blocks")
		}
	}
	//Since the file is striped, we have to reconstruct each stripe
	//for each stripe we rejoin the data
	e.ConStripes = e.MemSize * GiB / int(e.dataStripeSize)
	e.ConStripes = minInt(e.ConStripes, stripeNum)
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	stripeCnt := 0
	nextStripe := 0

	var scheme [][]int
	if options.Scheme == FIRST_K {
		scheme = e.findFirstKScheme(dist, replaceMap)
	} else if options.Scheme == FASTEST_K {
		scheme = e.findFastestKScheme(dist, replaceMap)
	} else if options.Scheme == RANDOM_K {
		scheme = e.findRandomScheme(dist, replaceMap)
	} else if options.Scheme == BALANCE_K {
		scheme = e.findBalanceScheme(dist, replaceMap)
	} else {
		scheme = e.findFirstKScheme(dist, replaceMap)
	}
	blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))

	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.ConStripes > stripeNum {
			nextStripe = stripeNum - stripeCnt
		} else {
			nextStripe = e.ConStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		for s := 0; s < nextStripe; s++ {
			s := s
			stripeNo := stripeCnt + s
			// eg.Go(
			func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// get dist and blockToOffset by stripeNo

				failList := make(map[int]bool)
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[stripeNo][i]
					disk := e.diskInfos[diskId]
					blkStat := fi.blockInfos[stripeNo][i]
					if !disk.available || blkStat.bstat != blkOK {
						failList[diskId] = true
						continue
					}
					erg.Go(func() error {
						//we also need to know the block's accurate offset with respect to disk
						offset := fi.blockToOffset[stripeNo][i]
						_, err := ifs[diskId].ReadAt(blobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
							int64(offset)*e.BlockSize)
						// fmt.Println("Read ", n, " bytes at", i, ", block ", block)
						if err != nil && err != io.EOF {
							return err
						}
						return nil
					})
				}
				if err := erg.Wait(); err != nil {
					return err
				}
				//Split the blob into k+m parts
				splitData, err := e.splitStripe(blobBuf[s])
				if err != nil {
					return err
				}
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				if !ok {
					err = e.enc.ReconstructWithKBlocks(
						splitData,
						&failList,
						&scheme[stripeNo],
						&(dist[stripeNo]),
						options.Degrade)
					if err != nil {
						return err
					}
					//write the Blob to restore paths
					egp := e.errgroupPool.Get().(*errgroup.Group)
					defer e.errgroupPool.Put(egp)
					for i := 0; i < e.K+e.M; i++ {
						i := i
						diskId := dist[stripeNo][i]
						if v, ok := replaceMap[diskId]; ok {
							restoreId := v - e.DiskNum
							writeOffset := fi.blockToOffset[stripeNo][i]
							egp.Go(func() error {
								_, err := rfs[restoreId].WriteAt(splitData[i],
									int64(writeOffset)*e.BlockSize)
								if err != nil {
									return err
								}
								if e.diskInfos[diskId].ifMetaExist {
									newMetapath := filepath.Join(e.diskInfos[restoreId].mntPath, "META")
									if _, err := copyFile(e.ConfigFile, newMetapath); err != nil {
										return err
									}
								}
								return nil

							})

						}
					}
					if err := egp.Wait(); err != nil {
						return err
					}
				}
				return nil
			}()
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe
	}

	//err = e.updateDiskPath(replaceMap)
	// if err != nil {
	// 	return nil, err
	// }
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}
