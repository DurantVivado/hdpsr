package hdpsr

import (
	"sync"

	"github.com/YuchongHu/reedsolomon"
)

// type CustomerAPI interface {
// 	Read(filename string) ([]byte, error)
// 	Write(filename string) (bool, error)
// 	ReadAll(filename []string) ([][]byte, error)
// 	WriteAll(filename []string) (bool, error)
// 	Delete(filename string) (bool, error)
// 	Change(filename string) (bool, error) //change file's meta
// }
// type HDRInfo struct {
// 	Used    uint64
// 	Free    uint64
// 	Total   uint64
// 	filenum int
// }

// diskInfo contains the disk-level information
// but not mechanical and electrical parameters
type diskInfo struct {
	// id
	diskId int64

	//the disk path
	mntPath string

	//it's flag and when disk fails, it renders false.
	available bool

	//it tells how many blocks a disk holds
	numBlocks int

	//it's a disk with meta file?
	ifMetaExist bool

	//the capacity of a disk (in Bytes)
	capacity int64

	//the partition of a disk
	partition string

	// the latency of a disk (in seconds)
	latency float64

	// read bandwidth of a disk (in B/s)
	read_bw float64

	// write bandwidth of a disk (in B/s)
	write_bw float64

	// randread bandwidth of a disk (in B/s)
	randread_bw float64

	// randwrite bandwidth of a disk (in B/s)
	randwrite_bw float64

	// if this disk is slow
	slow bool

	// if this disk is busy
	busy bool
}

// Erasure is the critical erasure coding structure
// contains fundamental encode/decode parameters
type Erasure struct {
	// the number of data blocks in a stripe
	K int `json:"dataShards"`

	// the number of parity blocks in a stripe
	M int `json:"parityShards"`

	// the block size. default to 4KiB
	BlockSize int64 `json:"blockSize"`

	// the disk number, only the first diskNum disks are used in mntPathFile
	DiskNum int `json:"diskNum"`

	//FileMeta lists, indicating fileName, fileSize, fileHash, fileDist...
	FileMeta []*fileInfo `json:"fileLists"`

	//how many stripes are allowed to encode/decode concurrently
	ConStripes int `json:"-"`

	// the replication factor for config file
	ReplicateFactor int

	// the reedsolomon streaming encoder, for streaming access
	sEnc reedsolomon.StreamEncoder

	// the reedsolomon encoder, for block access
	enc reedsolomon.Encoder

	// the data stripe size, equal to k*bs
	dataStripeSize int64

	// the data plus parity stripe size, equal to (k+m)*bs
	allStripeSize int64

	// diskInfo lists
	diskInfos []*diskInfo

	// configuration file path
	ConfigFile string `json:"-"`

	//file map
	fileMap sync.Map

	// the number of stripes
	StripeNum int64 `json:"stripeNum"`

	// stripeList
	Stripes []*stripeInfo `json:"stripeList"`

	// stripe id
	StripeInDisk [][]int64 `json:"stripeInDisk"`

	// the path of file recording all disks path
	DiskMountPath string `json:"-"`

	// DiskBWPath is the file path recording all disks' read bandwidth and write bandwidth
	DiskBWPath string `json:"-"`

	// whether to read bandwith from file
	ReadBWfromFile bool `json:"-"`

	// whether or not to override former files or directories, default to false
	Override bool `json:"-"`

	// errgroup pool
	errgroupPool sync.Pool

	// mutex
	mu sync.RWMutex

	//whether or not to mute outputs
	Quiet bool `json:"-"`

	// memory size
	MemSize int `json:"memSize"`

	// slowNum
	SlowNum int `json:"slowNum"`
}

// fileInfo defines the file-level information,
// it's concurrently safe
type fileInfo struct {
	//fild ID
	FileId int64 `json:"fileId"`

	//file name
	FileName string `json:"fileName"`

	//file size
	FileSize int64 `json:"fileSize"`

	//hash value (SHA256 by default)
	Hash string `json:"fileHash"`

	//distribution forms a block->disk mapping
	Distribution [][]int `json:"fileDist"`

	//blockToOffset has the same row and column number as Distribution but points to the block offset relative to a disk.
	blockToOffset [][]int

	//block state, default to blkOK otherwise blkFail in case of bit-rot.
	blockInfos [][]*blockInfo

	//system-level file info
	// metaInfo     *os.fileInfo

	//loadBalancedScheme is the most load-balanced scheme derived by SGA algo
	loadBalancedScheme [][]int
}

type stripeInfo struct {
	// ID of a stripe
	StripeId int64 `json:"stripeId"`

	// how many elem in DistBit
	DistNum int `json:"distNum"`

	// the distribution of a stripe, unsigned integer representation
	DistBit []uint64 `json:"distBit"`

	// the distribution of a stripe
	Dist []int `json:"dist"`

	// the same meaning
	BlockToOffset []int `json:"blockToOffset"`
}

type blockStat uint8
type blockInfo struct {
	bstat blockStat
}

// Options define the parameters for read and recover mode
type Options struct {
	//Degrade tells if degrade read is on
	Degrade bool
}

// SimOptions defines the parameters for simulation
type SimOptions struct {
	//switch between "diskFail" and "bitRot"
	Mode string
	// specify which disks to fail
	FailDisk string
	// specify number of disks to fail
	FailNum int
	//specify the fileName, used only for "bitRot" mode
	FileName string
}

// global system-level variables
var (
	err error
)

// constant variables
const (
	blkOK         blockStat = 0
	blkFail       blockStat = 1
	tempFile                = "./test/file.temp"
	maxGoroutines           = 10240
	intBit                  = 64
	GiB                     = 1024 * 1024 * 1024
	MiB                     = 1024 * 1024
	KiB                     = 1024
)

//templates
// const (
// 	config_templ = `
// This file is automatically generated, DO NOT EDIT
// System-level  Parameters:
// dataShards(k): {.k}
// dataShards(k): {.m}
// blockSize(bytes): {.blockSize}
// 	`

// 	file_templ = `
// FileName: {{.fileName}}
// Size(bytes): {{.fileSize}}
// SHA256SUM: {{.hash}}
// Distribution: {{.distribution}}
// `
// )
