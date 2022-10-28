package hdpsr

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// A Go-version Set
type IntSet map[int]struct{}

func (is *IntSet) Insert(x int) {
	if *is == nil {
		*is = make(IntSet)
	}
	(*is)[x] = struct{}{}
}
func (is *IntSet) Exist(x int) bool {
	if *is == nil {
		return false
	}
	_, ok := (*is)[x]
	return ok
}
func (is *IntSet) Erase(x int) {
	if *is == nil {
		return
	}
	if !is.Exist(x) {
		return
	}
	delete(*is, x)
}
func (is *IntSet) Clear() {
	if *is == nil {
		return
	}

	for k, _ := range *is {
		delete(*is, k)
	}
}
func (is *IntSet) Empty() bool {
	if *is == nil {
		return true
	}
	return len(*is) == 0
}
func (is *IntSet) Size() int {
	if *is == nil {
		return 0
	}
	return len(*is)
}

func sumInt(arrs []int, base int) int {
	sum := base
	for i := range arrs {
		sum += arrs[i]
	}
	return sum
}

// consult user to avoid maloperation
func consultUserBeforeAction() (bool, error) {
	fmt.Println("If you are sure to proceed, type:\n [Y]es or [N]o.")
	inputReader := bufio.NewReader(os.Stdin)
	for {
		ans, err := inputReader.ReadString('\n')
		if err != nil {
			return false, err
		}
		ans = strings.TrimSuffix(ans, "\n")
		if ans == "Y" || ans == "y" || ans == "Yes" || ans == "yes" {
			return true, nil
		} else if ans == "N" || ans == "n" || ans == "No" || ans == "no" {
			return false, nil
		} else {
			fmt.Println("Please do not make joke")
		}
	}

}

//an instant error handler

// look if path exists
func pathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// ceilFrac return (a+b-1)/b
func ceilFracInt(a, b int) int {
	return (a + b - 1) / b
}

// ceilFrac return (a+b-1)/b
func ceilFracInt64(a, b int64) int64 {
	return (a + b - 1) / b
}

func minInt(args ...int) int {
	if len(args) == 0 {
		return 0x7fffffff
	}
	ret := args[0]
	for _, arg := range args {
		if arg < ret {
			ret = arg
		}
	}
	return ret
}

func maxInt(args ...int) int {
	if len(args) == 0 {
		return 0xffffffff
	}
	ret := args[0]
	for _, arg := range args {
		if arg > ret {
			ret = arg
		}
	}
	return ret
}

func minFloat64(args ...float64) float64 {
	if len(args) == 0 {
		return 0
	}
	ret := args[0]
	for _, arg := range args {
		if arg < ret {
			ret = arg
		}
	}
	return ret
}

func maxFloat64(args ...float64) float64 {
	if len(args) == 0 {
		return 0
	}
	ret := args[0]
	for _, arg := range args {
		if arg > ret {
			ret = arg
		}
	}
	return ret
}

func sumFloat64(args ...float64) float64 {
	if len(args) == 0 {
		return 0
	}
	ret := float64(0)
	for _, arg := range args {
		ret += arg
	}
	return ret
}

// genRandArrInt generate a random integer array ranging in [start, strat+n)
func genRandArrInt(n, start int) []int {
	shuff := make([]int, n)
	for i := 0; i < n; i++ {
		shuff[i] = i + start
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
	return shuff
}

// genRandArrInt generates a random float64 array ranging in [base, base+max)
func genRandArrFloat64(n int, max int, base float64) []float64 {
	shuff := make([]float64, n)
	rand.Seed(1314520)
	for i := 0; i < n; i++ {
		shuff[i] = float64(max)*rand.Float64() + base
	}
	return shuff
}

// get arr of default sequence
func getSeqArr(n int) []int {
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = i
	}
	return out
}

// classical robin-round style
// e.g.
// 1 2 3 4 5
// 5 1 2 3 4
// 4 5 1 2 3
// ...
func rightRotateLayout(row, col int) [][]int {
	arr2D := make([][]int, row)
	for i := 0; i < row; i++ {
		arr2D[i] = make([]int, col)
		for j := 0; j < col; j++ {
			arr2D[i][j] = (j - i + col) % col
		}
	}
	return arr2D
}

func monitorCancel(cancel context.CancelFunc) {
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, syscall.SIGINT, syscall.SIGTERM)
	<-channel
	cancel()
}

func goroutineNum() int {
	return runtime.NumGoroutine()
}

// make an 2D byte slice
func makeArr2DByte(row, col int) [][]byte {
	out := make([][]byte, row)
	for i := range out {
		out[i] = make([]byte, col)
	}
	return out
}

// make an 3D byte slice
func makeArr3DByte(x, y, z int) [][][]byte {
	out := make([][][]byte, x)
	for i := range out {
		out[i] = make([][]byte, y)
		for j := range out[i] {
			out[i][j] = make([]byte, z)
		}
	}
	return out
}

// make an 2D int slice
func makeArr2DInt(row, col int) [][]int {
	out := make([][]int, row)
	for i := range out {
		out[i] = make([]int, col)
	}
	return out
}

// check if two file are completely same
// warning: use io.copy
func checkFileIfSame(dst, src string) (bool, error) {
	if ok, err := pathExist(dst); err != nil || !ok {
		return false, err
	}
	if ok, err := pathExist(src); err != nil || !ok {
		return false, err
	}
	fdst, err := os.Open(dst)
	if err != nil {
		return false, err
	}
	defer fdst.Close()
	fsrc, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer fsrc.Close()
	hashDst, err := hashStr(fdst)
	if err != nil {
		return false, err
	}
	hashSrc, err := hashStr(fsrc)
	if err != nil {
		return false, err
	}
	return hashDst == hashSrc, nil
}

// retain hashstr
func hashStr(f *os.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	out := fmt.Sprintf("%x", h.Sum(nil))
	return out, nil
}

// fillRandom
func fillRandom(p []byte) {
	for i := 0; i < len(p); i += 7 {
		val := rand.Int()
		for j := 0; i+j < len(p) && j < 7; j++ {
			p[i+j] = byte(val)
			val >>= 8
		}
	}
}

// string2Slice
func stringToSlice2D(s string) [][]int {
	s = strings.Trim(s, "[]\n")
	strs := strings.Split(s, ",")
	row := len(strs)
	out := make([][]int, row)
	for i := 0; i < row; i++ {
		sub := strings.Trim(strs[i], "[]\n")
		for _, num := range strings.Split(sub, ",") {
			n, _ := strconv.Atoi(num)
			out[i] = append(out[i], n)
		}
	}
	return out
}

// copyfile
func copyFile(srcFile, destFile string) (int64, error) {
	file1, err := os.Open(srcFile)
	if err != nil {
		return 0, err
	}
	defer file1.Close()
	file2, err := os.OpenFile(destFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return 0, err
	}
	defer file2.Close()
	return io.Copy(file2, file1)
}

// execShell will return the string appearing before the first "\n"
func execShell(command string) (string, error) {
	cmd := exec.Command("/bin/bash", "-c", command)

	stdout, err := cmd.StdoutPipe()
	defer stdout.Close()
	if err != nil {
		return "", err
	}

	if err := cmd.Start(); err != nil {
		return "", err
	}

	result, _ := ioutil.ReadAll(stdout)

	if err := cmd.Wait(); err != nil {
		return "", err
	}
	retstr := strings.Split(string(result), "\n")[0]
	return retstr, nil
}

func parsePartition(partInfo string) (string, error) {
	if len(partInfo) == 0 {
		return "", errPartInfoNotFound
	}

	partInfo_array := strings.Split(partInfo, "\n")

	partName := ""
	for i, str := range partInfo_array {
		if strings.Contains(str, "Filesystem") {
			keys := strings.Fields(str)
			values := strings.Fields(partInfo_array[i+1])
			for j := range keys {
				if keys[j] == "Filesystem" {
					partName = values[j]
				}
			}
		}
	}
	if partName == "" {
		return "", errPartInfoNotFound
	}
	return partName, nil
}

func parseIoStat(iostat string) (float64, float64, error) {
	if len(iostat) == 0 {
		return 0, 0, errIoStatNotFound
	}

	iostat_arr := strings.Split(iostat, "\n")

	await := -1.0
	svctm := -1.0
	flag := 0

	for i, str := range iostat_arr {
		if strings.Contains(str, "await") {
			keys := strings.Fields(str)
			values := strings.Fields(iostat_arr[i+1])

			for j := range keys {
				if keys[j] == "await" {
					await, err = strconv.ParseFloat(values[j], 64)
					flag++
				} else if keys[j] == "svctm" {
					svctm, err = strconv.ParseFloat(values[j], 64)
					flag++
				}
			}
		}
	}

	if flag < 2 {
		return 0, 0, errIoStatNotFound
	}
	return await, svctm, nil
}

// sliceMax returns the max value of a slice
func sliceMax(slice []float64) float64 {
	var max float64 = 0
	for i := range slice {
		if max < slice[i] {
			max = slice[i]
		}
	}
	return max
}

// sliceSum returns the sum of `slice“
func sliceSum(slice []float64) float64 {
	var res float64 = 0
	for i := range slice {
		res += slice[i]
	}
	return res
}

// sliceMinIndex returns the min value of `slice`
func sliceMinIndex(slice []float64) (float64, int) {
	if len(slice) == 0 {
		return 0, -1
	}
	var res float64 = slice[0]
	var index int = 0
	for i := 1; i < len(slice); i++ {
		if slice[i] < res {
			res = slice[i]
			index = i
		}
	}
	return res, index
}

// sliceSub substracts `sub` from each value of `slice`
func sliceSub(slice []float64, sub float64) {
	for i := range slice {
		slice[i] -= sub
	}
}

// sort2DArray sorts each line of `data` in reversed order
func sort2DArray(data [][]float64) {
	for i := range data {
		sort.Sort(sort.Reverse(sort.Float64Slice(data[i])))
	}
}

// getDiskUsage returns the disk usage in blocksize `bs`
func getDiskUsage(blkdev string, bs int64) (size int64, used int64, avail int64) {
	size_cmd := fmt.Sprintf(" df %s -B %d | sed -n 2p | awk -F ' ' '{print $2}'", blkdev, bs)
	ret, err := execShell(size_cmd)
	if err != nil {
		log.Fatal(err)
	}
	size, err = strconv.ParseInt(ret, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	used_cmd := fmt.Sprintf(" df %s -B %d | sed -n 2p | awk -F ' ' '{print $3}'", blkdev, bs)
	ret, err = execShell(used_cmd)
	if err != nil {
		log.Fatal(err)
	}
	used, err = strconv.ParseInt(ret, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	avail_cmd := fmt.Sprintf(" df %s -B %d | sed -n 2p | awk -F ' ' '{print $4}'", blkdev, bs)
	ret, err = execShell(avail_cmd)
	if err != nil {
		log.Fatal(err)
	}
	avail, err = strconv.ParseInt(ret, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return
}

// getMemUsage returns the memory usage in gigabytes
func getMemUsage() (total int64, used int64, free int64) {
	total_cmd := "free --giga | grep Mem | awk -F ' ' '{print $2}'"
	ret, err := execShell(total_cmd)
	if err != nil {
		log.Fatal(err)
	}
	total, err = strconv.ParseInt(ret, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	used_cmd := "free --giga | grep Mem | awk -F ' ' '{print $3}'"
	ret, err = execShell(used_cmd)
	if err != nil {
		log.Fatal(err)
	}
	used, err = strconv.ParseInt(ret, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	free_cmd := "free --giga | grep Mem | awk -F ' ' '{print $4}'"
	ret, err = execShell(free_cmd)
	if err != nil {
		log.Fatal(err)
	}
	free, err = strconv.ParseInt(ret, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return
}
