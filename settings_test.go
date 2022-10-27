// This exhibits the test file setups and parameters sets
//

package hdpsr

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
)

func Init() {
	log.Println("In testing mode, the system is default to reset, please keep in mind")
}

var (
	testDiskMountPath = filepath.Join("test", ".hdr.disks.path")
	testConfigFile    = filepath.Join("test", ".test.conf.json")
	testDiskBWPath    = filepath.Join("test", ".test.diskBW")
)

//randomly generate file of different size and encode them into HDR system

var dataShards = []int{

	2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
}
var parityShards = []int{
	2, 3, 4,
}

var fileSizesV1 = []int64{
	128, 256, 512, 1024,
	128 * KiB, 256 * KiB, 512 * KiB,
	1 * MiB, 4 * MiB, 16 * MiB, 32 * MiB, 64 * MiB,
}
var fileSizesV2 = []int64{

	128 * MiB, 256 * MiB, 512 * MiB, 1024 * MiB,
}
var blockSizesV1 = []int64{
	4 * KiB, 16 * KiB, 64 * KiB,
	256 * KiB, 512 * KiB,
}

var blockSizesV2 = []int64{
	1 * MiB, 2 * MiB, 4 * MiB, 8 * MiB, 16 * MiB, 32 * MiB, 64 * MiB, 128 * MiB,
	256 * MiB,
}

// genTempDir creates /input and /output dir in workspace root
func genTempDir() {
	if ok, err := pathExist("input"); !ok && err == nil {
		if err := os.Mkdir("input", 0644); err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}
	if ok, err := pathExist("output"); !ok && err == nil {
		if err := os.Mkdir("output", 0644); err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}
}

// delTempDir removes /input and /output dir in workspace root
func delTempDir() {
	if ok, err := pathExist("input"); ok && err == nil {
		if err := os.RemoveAll("input"); err != nil {
			log.Fatal(err)
		}
		fmt.Println("/input deleted")
	} else if err != nil {
		log.Fatal(err)
	}

	if ok, err := pathExist("output"); ok && err == nil {
		if err := os.RemoveAll("output"); err != nil {
			log.Fatal(err)
		}
		fmt.Println("/output deleted")
	} else if err != nil {
		log.Fatal(err)
	}

}

// generateRandomFileSize generate `num` files within range [minSize, maxSize]
func generateRandomFileSize(minSize, maxSize int64, num int) []int64 {
	out := make([]int64, num)
	for i := 0; i < num; i++ {
		out[i] = rand.Int63()%(maxSize-minSize) + minSize
	}
	return out
}

// generateRandomFileBySize generates a named  file with `fileSize` bytes.
func generateRandomFileBySize(filename string, fileSize int64) error {

	if ex, err := pathExist(filename); ex && err == nil {
		return nil
	} else if err != nil {
		return err
	}
	genTempDir()
	buf := make([]byte, fileSize)
	fillRandom(buf)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

// deleteTempFiles deletes temporary generated files as well as folders
func deleteTempFiles(tempFileSizes []int64) {
	for _, fileSize := range tempFileSizes {
		inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
		outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
		if ex, _ := pathExist(inpath); !ex {
			continue
		}
		err = os.Remove(inpath)
		if err != nil {
			log.Fatal(err)
		}
		if ex, _ := pathExist(outpath); !ex {
			continue
		}
		err = os.Remove(outpath)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// deleteTempFilesGroup deletes temporary generated file groups
func deleteTempFileGroup(inpath, outpath []string) {
	for i := range inpath {
		if ex, _ := pathExist(inpath[i]); !ex {
			continue
		}
		err = os.Remove(inpath[i])
		if err != nil {
			log.Fatal(err)
		}
		if ex, _ := pathExist(outpath[i]); !ex {
			continue
		}
		err = os.Remove(outpath[i])
		if err != nil {
			log.Fatal(err)
		}
	}
}
