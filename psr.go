package hdpsr

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

func (e *Erasure) PartialStripeRecover(fileName string, options *Options) (map[string]string, error) {
	// start1 := time.Now()
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
	if !e.Quiet {
		log.Println("start reconstructing blocks")
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

	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	// the number of parts to which the stripe repair process is split
	group := 2
	blobBuf := makeArr3DByte(e.ConStripes, group, int(e.BlockSize))
	stripeCnt := 0
	nextStripe := 0

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
			func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// get dist and blockToOffset by stripeNo
				fail := 0
				// get decodeMatrix of each stripe
				invalidIndices := make([]int, 0)
				failList := make(map[int]bool)
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[stripeNo][i]
					disk := e.diskInfos[diskId]
					blkStat := fi.blockInfos[stripeNo][i]
					if !disk.available || blkStat.bstat != blkOK {
						failList[diskId] = true
						invalidIndices = append(invalidIndices, i)
						continue
					}
				}
				tempShards := make([][]byte, len(invalidIndices))
				decodeMatrix, err := e.enc.GetDecodeMatrix(invalidIndices)
				if err != nil {
					return err
				}
				for i := 0; i < ceilFracInt(e.K, group); i++ {
					for j := 0; j < group; j++ {
						m := j
						d := i*group + m + fail // blk subscript
						if d >= e.K+e.M {
							break
						}
						diskId := dist[stripeNo][d]
						disk := e.diskInfos[diskId]
						if !disk.available {
							fail++
							j--
							continue
						}
						erg.Go(func() error {
							offset := fi.blockToOffset[stripeNo][d]
							_, err := ifs[diskId].ReadAt(blobBuf[s][m][0:e.BlockSize],
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
					inputsIdx := make([]int, 0)
					for k := i * group; k < (i+1)*group; k++ {
						if k > e.K+e.M {
							break
						}
						inputsIdx = append(inputsIdx, k)
					}
					// recoverWithSomeShards
					tempShards, err = e.enc.MultiRecoverWithSomeShards(
						decodeMatrix,
						blobBuf[s],
						inputsIdx,
						invalidIndices,
						tempShards)
					if err != nil {
						return err
					}
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
						tempId := 0
						egp.Go(func() error {
							_, err := rfs[restoreId].WriteAt(tempShards[tempId],
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
						tempId++
					}
				}
				if err := egp.Wait(); err != nil {
					return err
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
	// fmt.Println("second phase costs: ", time.Since(start2).Seconds())

	// start3 := time.Now()
	//err = e.updateDiskPath(replaceMap)
	// fmt.Println("third phase costs: ", time.Since(start3).Seconds())
	// if err != nil {
	// 	return nil, err
	// }
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}
