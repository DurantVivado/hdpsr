package hdpsr

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

func (e *Erasure) FullStripeRecover(filename string, options *Options) (
	map[string]string, error) {
	baseFileName := filepath.Base(filename)
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
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
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
			// offset := int64(subCnt) * e.allStripeSize
			eg.Go(func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				//read all blocks in parallel
				//We only have to read k blocks to rec
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
				//verify and reconstruct if broken
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				if !ok {
					// fmt.Println("reconstruct data of stripe:", stripeNo)
					err = e.enc.ReconstructWithList(splitData,
						&failList,
						&(fi.Distribution[stripeNo]),
						options.Degrade)

					// err = e.enc.ReconstructWithKBlocks(splitData,
					// 	&failList,
					// 	&loadBalancedScheme[stripeNo],
					// 	&(fi.Distribution[stripeNo]),
					// 	degrade)
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
			})

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
		log.Println("Finish recovering using Baseline")
	}
	return ReplaceMap, nil
}
