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

func (e *Erasure) getMinimalTimeSequence(dist [][]int, replaceMap map[int]int) (
	stripeOrder map[int][]int, minTimeSlice int) {
	failStripeVec := []int{}
	stripeNum := len(dist)
	for s := 0; s < stripeNum; s++ {
		for i := 0; i < e.K+e.M; i++ {
			if _, ok := replaceMap[dist[s][i]]; ok {
				failStripeVec = append(failStripeVec, s)
				break
			}
		}
	}
	distBit := make([][]uint64, stripeNum) // every stripe has GROUPNUM * 64 bits
	for s := 0; s < stripeNum; s++ {
		mask := uint64(1)
		distBit[s] = make([]uint64, GROUPNUM)
		for j := 0; j < e.K+e.M; j++ {
			group := dist[s][j] / IntBit
			mask <<= uint64(dist[s][j] % IntBit)
			distBit[s][group] |= mask
		}
	}
	minTimeSlice = 1
	stripeOrder = make(map[int][]int)
	i := 0
	failStripeNum := len(failStripeVec)
	for i < failStripeNum {
		// if current stripe conflicts with the next stripe
		slice := []int{failStripeVec[i]}
		j := i + 1
		if j < failStripeNum && !isConflictBit(&distBit, i, j) {
			slice = append(slice, failStripeVec[j])
			j++
		}
		stripeOrder[minTimeSlice] = slice
		minTimeSlice++
		i = j
	}
	return
}
func (e *Erasure) getMinimalTimeStripeScheduled(dist [][]int, replaceMap map[int]int) (
	stripeOrder map[int][]int, minTimeSlice int) {
	graph := make(map[int][]int)
	failStripeSet := &IntSet{}
	edgeNum := 0
	stripeNum := len(dist)
	for s := 0; s < stripeNum; s++ {
		for i := 0; i < e.K+e.M; i++ {
			if _, ok := replaceMap[dist[s][i]]; ok {
				failStripeSet.Insert(s)
				break
			}
		}
	}
	distBit := make([][]uint64, stripeNum) // every stripe has GROUPNUM * 64 bits
	for s := 0; s < stripeNum; s++ {
		mask := uint64(1)
		distBit[s] = make([]uint64, GROUPNUM)
		for j := 0; j < e.K+e.M; j++ {
			group := dist[s][j] / IntBit
			mask <<= uint64(dist[s][j] % IntBit)
			distBit[s][group] |= mask
		}
	}
	for s1 := range *failStripeSet {
		for s2 := range *failStripeSet {
			//if two nodes conflicts, then add an edge
			if s1 < s2 && isConflictBit(&distBit, s1, s2) {
				graph[s1] = append(graph[s1], s2)
				graph[s2] = append(graph[s2], s1)
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
	for s := range *failStripeSet {
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
						stripeOrder[t] = append(stripeOrder[t], cur)
						break
					}
				}
			}
		}
	}
	minTimeSlice = len(stripeOrder)
	return
}

// Algorithm: SS_HDR
func (e *Erasure) SS_HDR(
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

	var stripeOrder map[int][]int
	var minTimeSlice int
	if options.Scheme == SS_HDR {
		stripeOrder, minTimeSlice = e.getMinimalTimeStripeScheduled(dist, replaceMap)
	} else if options.Scheme == SEQUENCE {
		stripeOrder, minTimeSlice = e.getMinimalTimeSequence(dist, replaceMap)
	} else {
		return nil, fmt.Errorf("unknown scheme: %d", options.Scheme)
	}
	// if !e.Quiet {
	log.Printf("minTimeSlice: %d\n", minTimeSlice)
	log.Printf("StripeOrder: %v\n", stripeOrder)
	// }

	for t := 1; t <= int(minTimeSlice); t++ {
		eg := e.errgroupPool.Get().(*errgroup.Group)
		concurrency := len(stripeOrder[t])
		blobBuf := makeArr2DByte(concurrency, int(e.allStripeSize))
		for c := 0; c < concurrency; c++ {
			//for each slot
			stripeNo := stripeOrder[t][c]
			c := c
			eg.Go(func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// read blocks in parallel
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
						offset := fi.blockToOffset[stripeNo][i]
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
					err = e.enc.ReconstructWithList(splitData,
						&failList,
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
			})

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
