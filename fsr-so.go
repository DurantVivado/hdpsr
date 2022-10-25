package hdpsr

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
)

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
func (e *Erasure) getMinimalTimeContinous(stripeRepairTime []float64) (
	stripeOrder [][]int, minTime float64) {
	if len(stripeRepairTime) == 0 {
		return nil, 0
	}
	Pr := (e.MemSize * GiB) / (e.K * int(e.dataStripeSize))
	fmt.Printf("Pr:%d\n", Pr)
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
		stripeOrder = make([][]int, Pr)
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
	Pr := (e.MemSize * GiB) / (e.K * int(e.dataStripeSize))
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

const RAND_TIMES = 100000

func (e *Erasure) getMinimalTimeRand(stripeRepairTime []float64) (
	stripeOrder [][]int, minTime float64) {
	if len(stripeRepairTime) == 0 {
		return nil, 0
	}
	Pr := (e.MemSize * GiB) / (e.K * int(e.dataStripeSize))
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
	fmt.Println("valid :", RAND_TIMES-fail)
	return
}

// Algorithm: FullStripeRecoverWithOrder
func (e *Erasure) FullStripeRecoverWithOrder(
	fileName string, slowLatency int, options *Options) (
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
	e.ConStripes = (e.MemSize * GiB) / int(e.dataStripeSize)
	e.ConStripes = minInt(e.ConStripes, stripeNum)
	if e.ConStripes == 0 {
		return nil, errors.New("memory size is too small")
	}
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
	stripeCnt := 0
	nextStripe := 0
	stripes := e.StripeInDisk[failDisk]

	stripeRepairTime := e.getStripeRepairtime(slowLatency)
	stripeOrder, minTime := e.getMinimalTimeGreedy(stripeRepairTime)
	if !e.Quiet {
		log.Printf("minTime: %.3f s\n", minTime)
	}
	concurrency := len(stripeOrder)
	dist := fi.distribution
	for t := 1; t <= concurrency; t++ {
		eg := e.errgroupPool.Get().(*errgroup.Group)
		strps := stripeOrder[t]
		blobBuf := makeArr2DByte(len(strps), int(e.allStripeSize))
		for s, stripeNo := range strps {
			stripeNo := stripeNo
			s := s
			eg.Go(func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				//read all blocks in parallel
				//We only have to read k blocks to rec
				failList := make(map[int]bool)
				for i := 0; i < e.K+e.M; i++ {
					i := i
					dist
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
						_, err := ifs[diskId].ReadAt(
							blobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
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
				// fmt.Printf("stripeNo:%d, %v\n", stripeNo, fi.loadBalancedScheme[stripeNo])
				err = e.enc.ReconstructWithKBlocks(splitData,
					&failList,
					&fi.loadBalancedScheme[stripeNo],
					&(fi.Distribution[stripeNo]),
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
								newMetapath := filepath.Join(e.diskInfos[restoreId].diskPath, "META")
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

	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}

func (e *Erasure) getStripeRepairtime(slowLatency int) []float64 {
	stripeRepairTime := make([]float64, len(e.Stripes))
	for i := range e.Stripes {
		stripe := e.Stripes[i]
		fail := 0
		maxTime := float64(0)
		blkTime := float64(0)
		for j := 0; j < e.K; j++ {
			diskId := stripe.Dist[j+fail]
			if !e.diskInfos[diskId].available {
				fail++
				j--
				continue
			}
			if e.diskInfos[diskId].slow {
				blkTime = float64(e.BlockSize)/e.diskInfos[j].read_bw + float64(slowLatency)
			} else {
				blkTime = float64(e.BlockSize) / e.diskInfos[j].read_bw
			}
			maxTime = maxFloat64(maxTime, blkTime)
		}
		stripeRepairTime[i] = maxTime
		fmt.Printf("i:%d, time:%.3f\n", i, stripeRepairTime[i])
	}
	return stripeRepairTime
}
