package hdpsr

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const GROUPNUM = 4 // support GROUPNUM * 64 disks

const (
	SEQUENCE = iota
	SS_HDR
)

func isConflictBit(distBit *[][]uint64, a, b int) bool {
	for i := 0; i < GROUPNUM; i++ {
		if (*distBit)[a][i]&(*distBit)[b][i] != 0 {
			return true
		}
	}
	return false
}

func (e *Erasure) getMinimalTimeSequence(scheme [][]*blockInfo, replaceMap map[int]int) (
	stripeOrder map[int][]int, minTimeSlice int) {
	if len(scheme) == 0 {
		return nil, 0
	}
	failStripeNum := len(scheme)
	distBit := make([][]uint64, failStripeNum) // every stripe has GROUPNUM * 64 bits
	for s := 0; s < failStripeNum; s++ {
		mask := uint64(1)
		distBit[s] = make([]uint64, GROUPNUM)
		for j := 0; j < e.K; j++ {
			group := scheme[s][j].diskId / IntBit
			mask <<= uint64(scheme[s][j].diskId % IntBit)
			distBit[s][group] |= mask
		}
	}
	minTimeSlice = 1
	stripeOrder = make(map[int][]int)
	i := 0
	for i < failStripeNum {
		// if current stripe conflicts with the next stripe
		slice := []int{scheme[i][0].stripeId}
		j := i + 1
		if j < failStripeNum && !isConflictBit(&distBit, i, j) {
			slice = append(slice, scheme[j][0].stripeId)
			j++
		}
		stripeOrder[minTimeSlice] = slice
		minTimeSlice++
		i = j
	}
	return
}

func (e *Erasure) getMinimalTimeStripeScheduled(scheme [][]*blockInfo, replaceMap map[int]int) (
	stripeOrder map[int][]int, minTimeSlice int) {
	graph := make(map[int][]int)
	if len(scheme) == 0 {
		return nil, 0
	}
	failStripeNum := len(scheme)
	distBit := make([][]uint64, failStripeNum) // every stripe has GROUPNUM * 64 bits
	for s := 0; s < failStripeNum; s++ {
		mask := uint64(1)
		distBit[s] = make([]uint64, GROUPNUM)
		for j := 0; j < e.K; j++ {
			group := scheme[s][j].diskId / IntBit
			mask <<= uint64(scheme[s][j].diskId % IntBit)
			distBit[s][group] |= mask
		}
	}
	edgeNum := 0
	for i := range scheme {
		for j := range scheme[i+1:] {
			//if two nodes conflicts, then add an edge
			if isConflictBit(&distBit, i, j) {
				graph[i] = append(graph[i], j)
				graph[j] = append(graph[j], i)
				edgeNum++
			}
		}
	}
	if edgeNum == 0 {
		return nil, 1
	}
	// minTimeSlice is the minimal time slices needed for recovery
	minTimeSlice = 0
	// record records the used color
	record := &IntSet{}
	// stripeColor marks the color of each stripe
	stripeColor := make(map[int]int)
	stripeOrder = make(map[int][]int)
	// we use the disk_vec generated from last step
	// to give assistance to coloring sequence
	maxStripe := (e.MemSize * GiB) / int(e.dataStripeSize)
	// fmt.Println("max stripe number:", maxStripe)
	cur, maxColor := 0, 0
	for s := range scheme {
		if _, ok := stripeColor[s]; !ok {
			cur = s
			maxColor = 0
			record.Clear()
			for _, neig := range graph[cur] {
				record.Insert(stripeColor[neig])
				maxColor = maxInt(maxColor, stripeColor[neig])
			}
			for t := 1; t <= maxColor+1; t++ {
				if !record.Exist(t) {
					stripeColor[cur] = t
					// consider the memory limit
					if len(stripeOrder[t]) < int(maxStripe) {
						stripeOrder[t] = append(stripeOrder[t], scheme[cur][0].stripeId)
						break
					}
				}
			}
		}
	}
	minTimeSlice = len(stripeOrder)
	return
}

// Algorithm: HybridRecover
func (e *Erasure) StripeScheduleRecover(
	fileName string, options *Options) (
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
	j := e.DiskNum
	// think what if backup also breaks down, future stuff
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			ReplaceMap[e.diskInfos[i].mntPath] = e.diskInfos[j].mntPath
			replaceMap[i] = j
			j++
		}
	}
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
	if options.WriteToBackup {
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
	}
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

	var stripeOrder map[int][]int
	var minTimeSlice int
	var scheme [][]*blockInfo
	//step 1: find repair scheme
	if options.Method1 == "FIRST_K" {
		scheme, _ = e.findFirstKScheme(fi, replaceMap)
	} else if options.Method1 == "FASTEST_K" {
		scheme, _ = e.findFastestKScheme(fi, replaceMap)
	} else if options.Method1 == "RANDOM_K" {
		scheme, _ = e.findRandomScheme(fi, replaceMap)
	} else if options.Method1 == "LB_HDR" {
		scheme, _ = e.findBalanceScheme(fi, replaceMap)
	} else {
		return nil, fmt.Errorf("unknown method1: %s", options.Method1)
	}
	//step 2: get stripe repaired order
	if options.Method2 == "SS_HDR" {
		stripeOrder, minTimeSlice = e.getMinimalTimeStripeScheduled(scheme, replaceMap)
	} else if options.Method2 == "SEQ" {
		stripeOrder, minTimeSlice = e.getMinimalTimeSequence(scheme, replaceMap)
	} else {
		return nil, fmt.Errorf("unknown method2: %s", options.Method2)
	}

	for t := 1; t <= int(minTimeSlice); t++ {
		eg := e.errgroupPool.Get().(*errgroup.Group)
		concurrency := len(stripeOrder[t])
		blobBuf := makeArr2DByte(concurrency, int(e.allStripeSize))
		for c := 0; c < concurrency; c++ {
			//for each slot
			stripeNo := stripeOrder[t][c]
			c := c
			func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// read blocks in parallel
				failList := make(map[int]bool)
				for i := 0; i < e.K; i++ {
					i := i
					diskId := scheme[stripeNo][i].diskId
					diskOffset := scheme[stripeNo][i].diskOffset
					// disk := e.diskInfos[diskId]
					// blkStat := fi.blockInfos[stripeNo][i]
					// if !disk.available { || blkStat.bstat != blkOK {
					// 	failList[diskId] = true
					// 	continue
					// }
					erg.Go(func() error {
						offset := diskOffset
						_, err := ifs[diskId].ReadAt(blobBuf[c][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
							int64(offset)*e.BlockSize)
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
				splitData, err := e.splitStripe(blobBuf[c])
				if err != nil {
					return err
				}
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				if !ok {
					selblks := []int{}
					for _, bi := range scheme[stripeNo] {
						selblks = append(selblks, bi.diskId)
					}
					err = e.enc.ReconstructWithKBlocks(splitData,
						&failList,
						&selblks,
						&(dist[stripeNo]),
						options.Degrade)
					if err != nil {
						return err
					}

					if options.WriteToBackup {
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
				}
				return nil
			}()

		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		e.errgroupPool.Put(eg)

	}
	// err = e.updateDiskPath(replaceMap)
	// if err != nil {
	// 	return nil, err
	// }
	if !e.Quiet {
		log.Println("Finish recovering (using FSR-SO)")
	}
	return ReplaceMap, nil
}
