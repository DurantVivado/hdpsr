package hdpsr

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"

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
func (e *Erasure) findFirstKScheme(diskFailList *map[int]bool) (
	firstKScheme [][]int) {
	stripeNum := len(e.Stripes)
	failStripeSet := &IntSet{}
	firstKScheme = make([][]int, stripeNum)
	maxLoad := 0
	sumDisk := make([]int, e.DiskNum)
	sumLoad := 0
	for s := 0; s < stripeNum; s++ {
		stripe := e.Stripes[s]
		for i := 0; i < e.K+e.M; i++ {
			if mapExist(*diskFailList, stripe.Dist[i]) {
				failStripeSet.Insert(s)
				break
			}
		}
	}
	for s := 0; s < stripeNum; s++ {
		stripe := e.Stripes[s]
		if failStripeSet.Exist(s) {
			for i := 0; i < e.K+e.M; i++ {
				diskId := stripe.Dist[i]
				if !mapExist(*diskFailList, diskId) {
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
			firstKScheme[s] = stripe.Dist
		}
	}
	if !e.Quiet {
		fmt.Printf("---------------FSR-1K Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	return
}

// the default scheme: for each stripe, read the fastest k blocks,
// the corresponding disk of which has the largest bandwidth
func (e *Erasure) findFastestKScheme(diskFailList *map[int]bool) (
	fastestKScheme [][]int) {
	stripeNum := len(e.Stripes)
	failStripeSet := &IntSet{}
	fastestKScheme = make([][]int, stripeNum)
	maxLoad := 0
	sumDisk := make([]int, e.DiskNum)
	sumLoad := 0
	for s := 0; s < stripeNum; s++ {
		stripe := e.Stripes[s]
		for i := 0; i < e.K+e.M; i++ {
			if mapExist(*diskFailList, stripe.Dist[i]) {
				failStripeSet.Insert(s)
				break
			}
		}
	}
	for s := 0; s < stripeNum; s++ {
		stripe := e.Stripes[s]
		if failStripeSet.Exist(s) {
			diskVec := make([]int, 0)
			for i := 0; i < e.K+e.M; i++ {
				diskId := stripe.Dist[i]
				if !mapExist(*diskFailList, diskId) {
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
			fastestKScheme[s] = stripe.Dist
		}
	}
	if !e.Quiet {
		fmt.Printf("---------------FSR-FK Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	return
}

// for each failed stripe, randomly pick up k blocks
func (e *Erasure) findRandomScheme(diskFailList *map[int]bool) (
	randomScheme [][]int) {
	stripeNum := len(e.Stripes)
	failStripeSet := &IntSet{}
	randomScheme = make([][]int, stripeNum)
	maxLoad := 0
	sumDisk := make([]int, e.DiskNum)
	sumLoad := 0
	for s := 0; s < stripeNum; s++ {
		stripe := e.Stripes[s]
		for i := 0; i < e.K+e.M; i++ {
			if mapExist(*diskFailList, stripe.Dist[i]) {
				failStripeSet.Insert(s)
				break
			}
		}
	}
	for s := 0; s < stripeNum; s++ {
		stripe := e.Stripes[s]
		if failStripeSet.Exist(s) {
			diskVec := make([]int, 0)
			for i := 0; i < e.K+e.M; i++ {
				diskId := stripe.Dist[i]
				if !mapExist(*diskFailList, diskId) {
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
			randomScheme[s] = stripe.Dist
		}
	}
	if !e.Quiet {
		fmt.Printf("---------------FSR-R Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	return
}

func (e *Erasure) findBalanceScheme(diskFailList *map[int]bool) (
	balanceScheme [][]int) {
	stripeNum := len(e.Stripes)
	failStripeNum := 0
	failStripeSet := &IntSet{}
	diskLoads := make([]int, e.DiskNum)
	diskDict := make([]IntSet, e.DiskNum)
	balanceScheme = make([][]int, stripeNum)
	stripeRedu := make(map[int]int)
	available := 0
	for s := 0; s < stripeNum; s++ {
		flag := true
		failBlk := 0
		stripe := e.Stripes[s]
		for i := 0; i < e.K+e.M; i++ {
			if mapExist(*diskFailList, stripe.Dist[i]) {
				if _, ok := stripeRedu[s]; ok {
					stripeRedu[s]--
				} else {
					stripeRedu[s] = e.M - 1
				}
				failBlk++
				flag = false
			} else {
				diskLoads[stripe.Dist[i]]++
				diskDict[stripe.Dist[i]].Insert(s)
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
			if !mapExist(*diskFailList, d) && !failReduList.Exist(d) &&
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
						if !failReduList.Exist(n) &&
							!mapExist(*diskFailList, n) &&
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
	maxLoad := 0
	sumDisk := make([]int, e.DiskNum)
	sumLoad := 0

	for s := 0; s < stripeNum; s++ {
		stripe := e.Stripes[s]
		if failStripeSet.Exist(s) {
			for i := 0; i < e.K+e.M; i++ {
				diskId := stripe.Dist[i]
				if !mapExist(*diskFailList, diskId) && diskDict[diskId].Exist(s) {

					balanceScheme[s] = append(balanceScheme[s], diskId)
					sumDisk[diskId]++
					maxLoad = maxInt(maxLoad, sumDisk[diskId])
					sumLoad++
				}
			}
		} else {
			balanceScheme[s] = stripe.Dist
		}
	}
	if !e.Quiet {
		fmt.Printf("---------------FSR-B Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	return
}

// the idea is to select k blocks from the stripe
func (e *Erasure) FullStripeRecoverBlockSelected(fileName string, options *Options) (
	map[string]string, error) {
	// start1 := time.Now()
	var failDisk int = 0
	for i := range e.diskInfos {
		if !e.diskInfos[i].available {
			failDisk = i
			break
		}
	}
	if !e.Quiet {
		log.Printf("Start recovering with stripe, totally %d stripes need recovery",
			len(e.StripeInDisk[failDisk]))
	}
	baseName := filepath.Base(fileName)
	//the failed disks are mapped to backup disks
	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)
	diskFailList := make(map[int]bool, 1)

	ReplaceMap[e.diskInfos[failDisk].mntPath] = e.diskInfos[e.DiskNum].mntPath
	replaceMap[failDisk] = e.DiskNum
	diskFailList[failDisk] = true

	// start recovering: recover all stripes in this disk

	// open all disks
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)
	// alive := int32(0)
	for i, disk := range e.diskInfos[0:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.mntPath, baseName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				ifs[i] = nil
				return nil
			}
			ifs[i], err = os.Open(blobPath)
			if err != nil {
				return err
			}

			disk.available = true
			// atomic.AddInt32(&alive, 1)
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("read failed %s", err.Error())
		}
	}
	defer func() {
		for i := 0; i < e.DiskNum; i++ {
			if ifs[i] != nil {
				ifs[i].Close()
			}
		}
	}()
	if !e.Quiet {
		log.Println("start reconstructing blocks")
	}

	// create BLOB in the backup disk
	disk := e.diskInfos[e.DiskNum]
	folderPath := filepath.Join(disk.mntPath, baseName)
	blobPath := filepath.Join(folderPath, "BLOB")
	if e.Override {
		if err := os.RemoveAll(folderPath); err != nil {
			return nil, err
		}
	}
	if err := os.Mkdir(folderPath, 0777); err != nil {
		return nil, errDataDirExist
	}
	rfs, err := os.OpenFile(blobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return nil, err
	}
	defer rfs.Close()

	// fmt.Println("first phase costs: ", time.Since(start1).Seconds())

	// start2 := time.Now()
	// read stripes every blob in parallel
	// read blocks every stripe in parallel
	stripeNum := len(e.StripeInDisk[failDisk])
	e.ConStripes = (e.MemSize * GiB) / int(e.allStripeSize)
	e.ConStripes = minInt(e.ConStripes, stripeNum)
	if e.ConStripes == 0 {
		return nil, errors.New("memory size is too small")
	}
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
	stripeCnt := 0
	nextStripe := 0
	failStripes := e.StripeInDisk[failDisk]

	var scheme [][]int
	if options.Scheme == FIRST_K {
		scheme = e.findFirstKScheme(&diskFailList)
	} else if options.Scheme == FASTEST_K {
		scheme = e.findFastestKScheme(&diskFailList)
	} else if options.Scheme == RANDOM_K {
		scheme = e.findRandomScheme(&diskFailList)
	} else if options.Scheme == BALANCE_K {
		scheme = e.findBalanceScheme(&diskFailList)
	} else {
		scheme = e.findFirstKScheme(&diskFailList)
	}
	// balanceScheme := e.findBalanceScheme(&diskFailList)

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
				// s := s
				spId := failStripes[stripeNo]
				spInfo := e.Stripes[spId]
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// get dist and blockToOffset by stripeNo
				dist := spInfo.Dist

				blockToOffset := spInfo.BlockToOffset
				tempShard := make([]byte, e.BlockSize)
				// get decodeMatrix of each stripe
				invalidIndice := -1
				for i, blk := range dist {
					if blk == failDisk {
						invalidIndice = i
						break
					}
				}
				invalidIndices := []int{invalidIndice}
				// invalidIndices = append(invalidIndices, invalidIndice)
				// get the decoded matrix of failed vector
				decodeMatrix, err := e.enc.GetDecodeMatrix(invalidIndices)
				if err != nil {
					return err
				}
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[i]
					disk := e.diskInfos[diskId]
					if !disk.available {
						continue
					}
					erg.Go(func() error {
						offset := blockToOffset[i]
						_, err := ifs[diskId].ReadAt(blobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
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
				splitData, err := e.splitStripe(blobBuf[s])
				if err != nil {
					return err
				}
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				if !ok {
					tempShard, err = e.enc.RecoverWithSomeShards(
						decodeMatrix,
						blobBuf,
						scheme[s],
						invalidIndice,
						tempShard,
					)
					if err != nil {
						return err
					}
				}
				//write the Blob to restore paths
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[i]
					if diskId == failDisk {
						writeOffset := blockToOffset[i]
						_, err := rfs.WriteAt(tempShard, int64(writeOffset)*e.BlockSize)
						if err != nil {
							return err
						}
						if e.diskInfos[diskId].ifMetaExist {
							newMetapath := filepath.Join(e.diskInfos[e.DiskNum].mntPath, "META")
							if _, err := copyFile(e.ConfigFile, newMetapath); err != nil {
								return err
							}
						}
						break
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
