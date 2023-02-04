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

// Algorithm: FullStripeRecoverWithOrder
func (e *Erasure) PartialStripeRecoverWithOrder(
	fileName string, slowLatency float64, options *Options) (
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
	stripeCnt := 0

	stripeRepairTime := e.getStripeRepairtime(dist, slowLatency)
	if !e.Quiet {
		fmt.Printf("stripeRepairTime: %v\n", stripeRepairTime)
	}
	var stripeOrder [][]int
	var minTime float64
	if options.Scheme == CONTINUOUS {
		stripeOrder, minTime = e.getMinimalTimeContinuous(stripeRepairTime)
	} else if options.Scheme == GREEDY {
		stripeOrder, minTime = e.getMinimalTimeGreedy(stripeRepairTime)
	} else if options.Scheme == RANDOM {
		stripeOrder, minTime = e.getMinimalTimeRand(stripeRepairTime)
	} else {
		return nil, fmt.Errorf("unknown scheme: %d", options.Scheme)
	}
	if !e.Quiet {
		log.Printf("minTime: %.3f s\n", minTime)
		log.Printf("StripeOrder: %v\n", stripeOrder)
	}
	// `concurrency` specifies how many groups of
	// stripes the memory can accommodate in the same time slot
	concurrency := len(stripeOrder)
	slotId := make([]int, concurrency)
	blobBuf := makeArr3DByte(concurrency, options.intraStripe, int(e.BlockSize))
	for stripeCnt < stripeNum {
		eg := e.errgroupPool.Get().(*errgroup.Group)
		for c := 0; c < concurrency; c++ {
			//for each slot
			if len(stripeOrder[c]) == 0 {
				continue
			}
			// if the current slot index reaches
			// the last block, continue
			if len(stripeOrder[c]) == slotId[c] {
				continue
			}
			stripeNo := stripeOrder[c][slotId[c]]
			blob := blobBuf[c]
			slotId[c]++
			stripeCnt++
			eg.Go(func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// get decodeMatrix of each stripe
				invalidIndices := make([]int, 0)
				for i, blk := range dist[stripeNo] {
					if _, ok := replaceMap[blk]; ok {
						invalidIndices = append(invalidIndices, i)
					}
				}
				tempShard := make([][]byte, len(invalidIndices))
				for i := 0; i < len(invalidIndices); i++ {
					tempShard[i] = make([]byte, e.BlockSize)
				}
				// invalidIndices = append(invalidIndices, invalidIndice)
				decodeMatrix, err := e.enc.GetDecodeMatrix(invalidIndices)
				if err != nil {
					return err
				}
				// read blocks in parallel
				stripeToDiskArr := make([]*sortNode, 0)
				fail := 0
				for i := 0; i < e.K; i++ {
					diskId := dist[stripeNo][i+fail]
					if !e.diskInfos[diskId].available {
						fail += 1
						i -= 1
						continue
					}
					stripeToDiskArr = append(stripeToDiskArr,
						&sortNode{
							diskId:  diskId,
							idx:     i,
							blockId: i + fail,
							latency: float64(e.BlockSize) / e.diskInfos[diskId].read_bw,
						})
				}

				for len(stripeToDiskArr) > 0 {
					// everytime sort stripeToDiskArr in reversed order
					// pick `options.intraStripe` disks with biggest latency
					group := BiggestK(stripeToDiskArr, options.intraStripe)
					for i := range group {
						i := i
						diskId := group[i].diskId
						blockId := group[i].blockId
						erg.Go(func() error {
							offset := fi.blockToOffset[stripeNo][blockId]
							_, err := ifs[diskId].ReadAt(blob[i][0:e.BlockSize],
								int64(offset)*e.BlockSize)
							if err != nil && err != io.EOF {
								return err
							}
							return nil
						})
					}
					if err = erg.Wait(); err != nil {
						return err
					}
					inputsIdx := make([]int, 0)
					for i := range group {
						inputsIdx = append(inputsIdx, int(group[i].idx))
					}
					tempShard, err = e.enc.MultiRecoverWithSomeShards(
						decodeMatrix,
						blob[:len(group)],
						inputsIdx,
						invalidIndices,
						tempShard)
					if err != nil {
						return err
					}
					// delete visited disk in stripeToDiskArr
					if options.intraStripe > len(stripeToDiskArr) {
						stripeToDiskArr = stripeToDiskArr[len(stripeToDiskArr):]
					} else {
						stripeToDiskArr = stripeToDiskArr[options.intraStripe:]
					}
				}
				// write the block to backup disk
				egp := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(egp)
				orderMap := make(map[int]int)
				tmp := 0
				for j := 0; j < len(invalidIndices); j++ {
					orderMap[dist[stripeNo][invalidIndices[j]]] = tmp
					tmp++
				}
				for i := 0; i < e.K+e.M; i++ {
					diskId := dist[stripeNo][i]
					v := 0
					ok := false
					if v, ok = replaceMap[diskId]; ok {
						diskId := diskId
						restoreId := v - e.DiskNum
						writeOffset := fi.blockToOffset[stripeNo][i]
						egp.Go(func() error {
							restoreId := restoreId
							writeOffset := writeOffset
							diskId := diskId
							tmpId := orderMap[diskId]
							// fmt.Printf("stripe %d disk %d tmpId: %v\n", spId, diskId, tmpId)
							_, err := rfs[restoreId].WriteAt(tempShard[tmpId], int64(writeOffset)*e.BlockSize)
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
