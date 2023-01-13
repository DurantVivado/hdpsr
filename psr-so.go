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
	fileName string, slowLatency int, options *Options) (
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
	concurrency := len(stripeOrder)
	slotId := make([]int, concurrency)
	blobBuf := makeArr2DByte(concurrency, int(e.allStripeSize))
	for stripeCnt < stripeNum {
		eg := e.errgroupPool.Get().(*errgroup.Group)
		for c := 0; c < concurrency; c++ {
			//for each slot
			if len(stripeOrder[c]) == 0 {
				continue
			}
			if len(stripeOrder[c]) == slotId[c] {
				continue
			}
			stripeNo := stripeOrder[c][slotId[c]]
			slotId[c]++
			stripeCnt++
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
