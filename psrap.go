package hdpsr

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
)

func reduceFirst(arr [][]float64, is int) []float64 {
	s := len(arr)
	if s == 0 {
		return nil
	}
	k := len(arr[0])
	if k == 0 {
		return nil
	}
	tmp := make([][]float64, 0)
	res := make([]float64, 0)
	for i := range arr {
		line := arr[i]
		t := make([]float64, 0)
		for i := 0; i < ceilFracInt(k-1, is-1); i++ {
			var start int
			if i == 0 {
				start = 0
			} else {
				start = is + (i-1)*(is-1)
			}
			end := is + i*(is-1)
			end = minInt(end, len(line))
			t = append(t, sliceMax(line[start:end]))
		}
		tmp = append(tmp, t)
	}
	for i := range tmp {
		res = append(res, sliceSum(tmp[i]))
	}
	return res
}

func reduceSecond(arr []float64, conStripe int) float64 {
	s := len(arr)
	if s == 0 {
		return 0
	}
	recovered := 0
	var time float64 = 0
	mem := arr[0:conStripe]
	index := conStripe
	for recovered < s {
		minVal, minIndex := sliceMinIndex(mem)
		if minIndex == -1 {
			fmt.Println("getMinIndex error")
			return 0
		}
		sliceSub(mem, minVal)
		time += minVal
		recovered++
		if index < s {
			mem[minIndex] = arr[index]
			index++
		}
	}
	return time
}

func reduce(data [][]float64, is int, mem int, blockSize int) float64 {
	if len(data) == 0 || len(data[0]) == 0 {
		return 0
	}
	// how many intrastripe can be put in the memory at the same time
	conStripe := mem / (is * blockSize)
	conStripe = minInt(conStripe, len(data))
	dedata := reduceFirst(data, is)
	time := reduceSecond(dedata, conStripe)
	return time
}

// Calculate the read time for each single block
func (e *Erasure) getData(slowLatency float64) [][]float64 {
	data := make([][]float64, len(e.Stripes))
	for i := range data {
		data[i] = make([]float64, e.K)
	}

	for i := range e.Stripes {
		stripe := e.Stripes[i]
		fail := 0
		for j := 0; j < e.K; j++ {
			diskId := stripe.Dist[j+fail]
			if !e.diskInfos[diskId].available {
				fail++
				j--
				continue
			}
			if e.diskInfos[diskId].slow {
				data[i][j] = float64(e.BlockSize)/e.diskInfos[j].read_bw + float64(slowLatency)
			} else {
				data[i][j] = float64(e.BlockSize) / e.diskInfos[j].read_bw
			}
		}
	}
	return data
}

func (e *Erasure) getIntraStripeOptimal(slowLatency float64) int {
	data := e.getData(slowLatency)
	sort2DArray(data)
	var minIs int = 2
	var minTime float64 = reduce(data, 2, e.MemSize*GiB, int(e.BlockSize))
	for is := 3; is <= e.K/2; is++ {
		time := reduce(data, is, e.MemSize*GiB, int(e.BlockSize))
		if time < minTime {
			minTime = time
			minIs = is
		}
	}
	time := reduce(data, e.K, e.MemSize*GiB, int(e.BlockSize))
	if minTime > time {
		minIs = e.K
	}
	return minIs
}

func (e *Erasure) PartialStripeRecoverPreliminary(fileName string, slowLatency float64, options *Options) (map[string]string, error) {
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
	if err != nil {
		return nil, err
	}
	baseName := filepath.Base(fileName)
	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)

	ReplaceMap[e.diskInfos[failDisk].mntPath] = e.diskInfos[e.DiskNum].mntPath
	replaceMap[failDisk] = e.DiskNum

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
	// fmt.Println(disk.mntPath)
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

	start := time.Now()
	// measure the bandwidth of each disk
	err = e.getDiskBWRead(ifs)
	if err != nil {
		return nil, err
	}
	intraStripe := e.getIntraStripeOptimal(slowLatency)
	t := time.Since(start).Seconds()
	// fmt.Println("psrap algorithm running time: ", t)
	logfile := "log.txt"
	file, err := os.OpenFile(logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil && err != io.EOF {
		panic(err)
	}
	str := fmt.Sprintf("    psrap algorithm running time: %f\n", t)
	file.Write([]byte(str))
	file.Close()

	// fmt.Println("intraStripe: ", intraStripe)

	start = time.Now()
	// read stripes every blob in parallel
	// read blocks every stripe in parallel
	stripeNum := len(e.StripeInDisk[failDisk])
	e.ConStripes = (e.MemSize * GiB) / (intraStripe * int(e.BlockSize))
	e.ConStripes = minInt(e.ConStripes, stripeNum)
	if e.ConStripes == 0 {
		return nil, errors.New("no stripes to be recovered or memory size is too small")
	}
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	blobBuf := makeArr3DByte(e.ConStripes, intraStripe, int(e.BlockSize))
	stripeCnt := 0
	nextStripe := 0
	stripes := e.StripeInDisk[failDisk]

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
			eg.Go(func() error {
				spId := stripes[stripeNo]
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
				// get the latency array
				stripeToDiskArr := make([]*sortNode, 0)
				fail := 0
				for i := 0; i < e.K; i++ {
					diskId := dist[i+fail]
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
							latency: e.diskInfos[diskId].latency,
						})
				}
				for len(stripeToDiskArr) > 0 {
					// get the biggest K disks of the stripe by latency
					group := BiggestK(stripeToDiskArr, intraStripe)
					for i := range group {
						i := i
						diskId := group[i].diskId
						blockId := group[i].blockId
						erg.Go(func() error {
							offset := blockToOffset[blockId]
							_, err := ifs[diskId].ReadAt(blobBuf[s][i][0:e.BlockSize],
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
						inputsIdx = append(inputsIdx,
							int(group[i].idx))
					}
					tempShard, err = e.enc.RecoverWithSomeShards(
						decodeMatrix,
						blobBuf[s][:len(group)],
						inputsIdx,
						invalidIndice,
						tempShard,
					)
					if err != nil {
						return err
					}
					// delete visited disk in stripeToDiskArr
					if intraStripe > len(stripeToDiskArr) {
						stripeToDiskArr = stripeToDiskArr[len(stripeToDiskArr):]
					} else {
						stripeToDiskArr = stripeToDiskArr[intraStripe:]
					}
				}
				// write the block to backup disk
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
				// fmt.Printf("stripe %d: transfer time: %v, total time: %v, ratio of transfer: %v\n", spId, transferTime, totalTime, transferTime/(totalTime+t))
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe
	}
	// fmt.Println("recover time: ", time.Since(start).Seconds())

	//err = e.updateDiskPath(replaceMap)
	// if err != nil {
	// 	return nil, err
	// }
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}
