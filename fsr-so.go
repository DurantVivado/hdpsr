package hdpsr

import (
	"container/heap"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	CONTINUOUS = 0
	GREEDY     = 1
	RANDOM     = 2
)

const RAND_TIMES = 200000

func findContinuousScheme(A []float64, mid float64, Pr int) (
	stripeOrder [][]int, minTime float64) {
	cnt := 0
	sum := float64(0)
	stripeOrder = make([][]int, Pr)
	maxSubTime := float64(0)
	for i := 0; i < len(A); i++ {
		if sum+A[i] > mid {
			cnt++
			minTime = maxFloat64(minTime, maxSubTime)
			sum = 0
			maxSubTime = 0
		}
		if cnt >= Pr {
			return nil, 0
		}
		sum += A[i]
		stripeOrder[cnt] = append(stripeOrder[cnt], i)
		maxSubTime += A[i]
	}
	return
}

// full-stripe-repair with stripe order first
func (e *Erasure) getMinimalTimeContinuous(stripeRepairTime []float64) (
	stripeOrder [][]int, minTime float64) {
	if len(stripeRepairTime) == 0 {
		return nil, 0
	}
	Pr := (e.MemSize * GiB) / int(e.allStripeSize)
	stripeOrder = make([][]int, Pr)
	if len(stripeRepairTime) <= Pr {
		for i := 0; i < len(stripeRepairTime); i++ {
			stripeOrder[i] = append(stripeOrder[i], i)
		}
		return stripeOrder, maxFloat64(stripeRepairTime...)
	}
	maxTime := maxFloat64(stripeRepairTime...)
	sumTime := sumFloat64(stripeRepairTime...)
	l, r := maxTime, sumTime

	for r-l > 1e-6 {
		mid := l + (r-l)/2
		ret1, ret2 := findContinuousScheme(stripeRepairTime, mid, Pr)
		if ret1 != nil {
			stripeOrder = ret1
			minTime = ret2
			r = mid
		} else {
			l = mid
		}
	}
	return
}

// the greedy heuristic is to prioritize the long enduring stripes, and
// every time put the slowest in the fastest slot.
func (e *Erasure) getMinimalTimeGreedy(stripeRepairTime []float64) (
	stripeOrder [][]int, minTime float64) {
	n := len(stripeRepairTime)
	if n == 0 {
		return nil, 0
	}
	Pr := (e.MemSize * GiB) / int(e.allStripeSize)
	stripeOrder = make([][]int, Pr)
	if n <= Pr {
		for i := 0; i < n; i++ {
			stripeOrder[i] = append(stripeOrder[i], i)
		}
		return stripeOrder, maxFloat64(stripeRepairTime...)
	}

	h := &HeapFloat64{bigTop: true}
	slots := &HeapFloat64{}
	for i := 0; i < Pr; i++ {
		heap.Push(slots, heapv{i, 0})
	}
	for i := 0; i < n; i++ {
		heap.Push(h, heapv{i, stripeRepairTime[i]})
	}
	for h.Len() > 0 {
		next_stripe := heap.Pop(h).(heapv)
		slot := heap.Pop(slots).(heapv)
		stripeOrder[slot.id] = append(stripeOrder[slot.id], next_stripe.id)
		minTime = maxFloat64(minTime, slot.val+stripeRepairTime[next_stripe.id])
		heap.Push(slots, heapv{
			slot.id, slot.val + stripeRepairTime[next_stripe.id],
		})
	}
	return
}

func (e *Erasure) getMinimalTimeRand(stripeRepairTime []float64) (
	stripeOrder [][]int, minTime float64) {
	if len(stripeRepairTime) == 0 {
		return nil, 0
	}
	Pr := (e.MemSize * GiB) / int(e.allStripeSize)
	stripeOrder = make([][]int, Pr)
	if len(stripeRepairTime) <= Pr {
		for i := 0; i < len(stripeRepairTime); i++ {
			stripeOrder[i] = append(stripeOrder[i], i)
		}
		return stripeOrder, maxFloat64(stripeRepairTime...)
	}
	// A random heuristic
	minTime = math.MaxFloat64
	fail := int64(0)
	for i := 0; i < RAND_TIMES; i++ {
		maxTime := float64(0)
		slotSet := &IntSet{}
		tempOrder := make([][]int, Pr)
		sumGroup := make([]float64, Pr)
		rand.Seed(time.Now().UnixNano())
		for j := 0; j < len(stripeRepairTime); j++ {
			slot := rand.Int() % Pr
			sumGroup[slot] += stripeRepairTime[j]
			tempOrder[slot] = append(tempOrder[slot], j)
			maxTime = maxFloat64(maxTime, sumGroup[slot])
			if maxTime > minTime {
				fail++
				break
			}
			slotSet.Insert(slot)
		}
		// fmt.Printf("minTime:%d\n", time.Now().UnixNano())
		if maxTime < minTime && slotSet.Size() == Pr {
			minTime = maxTime
			stripeOrder = tempOrder
		}
	}
	// fmt.Println("valid :", RAND_TIMES-fail)
	return
}

// Algorithm: FullStripeRecoverWithOrder
func (e *Erasure) FullStripeRecoverWithOrder(
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
		log.Println("FSR-SO_c")
		stripeOrder, minTime = e.getMinimalTimeContinuous(stripeRepairTime)
	} else if options.Scheme == GREEDY {
		log.Println("FSR-SO_g")
		stripeOrder, minTime = e.getMinimalTimeGreedy(stripeRepairTime)
	} else if options.Scheme == RANDOM {
		log.Println("FSR-SO_r")
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
			if len(stripeOrder[c]) == slotId[c] {
				continue
			}
			stripeNo := stripeOrder[c][slotId[c]]
			slotId[c]++
			stripeCnt++
			c := c
			func() error {
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

func (e *Erasure) getStripeRepairtime(dist [][]int, slowLatency int) []float64 {
	stripeRepairTime := make([]float64, len(e.Stripes))
	stripeNum := len(dist)
	for s := 0; s < stripeNum; s++ {
		maxTime := float64(0)
		blkTime := float64(0)
		for j := 0; j < e.K+e.M; j++ {
			diskId := dist[s][j]
			if !e.diskInfos[diskId].available {
				continue
			}
			if e.diskInfos[diskId].slow {
				blkTime = float64(e.BlockSize)/(e.diskInfos[diskId].read_bw*MiB) + float64(slowLatency)
			} else {
				blkTime = float64(e.BlockSize) / (e.diskInfos[diskId].read_bw * MiB)
			}
			maxTime = maxFloat64(maxTime, blkTime)
		}
		stripeRepairTime[s] = maxTime
		// fmt.Printf("i:%d, time:%f\n", i, stripeRepairTime[i])
	}
	return stripeRepairTime
}
