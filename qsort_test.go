package hdpsr

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBiggestK(t *testing.T) {
	rand.Seed(1084751307)
	Len := 10
	arr := make([]*sortNode, Len)
	maxLat := 5.0
	minLat := 0.0
	verifyArr := make([]float64, Len)
	for i := 0; i < 10; i++ {
		randF := rand.Float64() * (maxLat - minLat)
		arr[i] = &sortNode{
			diskId:  i,
			idx:     i,
			blockId: i,
			latency: randF,
		}
		verifyArr[i] = randF
	}
	fmt.Println("verifyArr:", verifyArr)
	ret := BiggestK(arr, 5)
	fmt.Println("ret:", ret)
}
