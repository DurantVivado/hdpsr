package hdpsr

import (
	"reflect"
	"testing"
)

func TestSmallTopHeapSortInt(t *testing.T) {
	testarr := []int{4, 3, 6, 0, -1, 4, 3, -9}
	expect := []int{-9, -1, 0, 3, 3, 4, 4, 6}
	if !reflect.DeepEqual(HeapSortInt(testarr), expect) {
		t.Fatal("HeapSort failed")
	}
}

func TestBigTopHeapSortFloat64(t *testing.T) {
	testarr := []float64{
		1.2210436506420845,
		2.082485723729168,
		2.8815392161530062,
		1.9831141204132763,
		1.9647316068318756,
		1.0936567454817911,
		1.4244619052503136,
		1.0997697112309897,
		1.5126855990009054,
		0.41462043869096243,
	}
	expect := []float64{
		0.41462043869096243,
		1.0936567454817911,
		1.0997697112309897,
		1.2210436506420845,
		1.4244619052503136,
		1.5126855990009054,
		1.9647316068318756,
		1.9831141204132763,
		2.082485723729168,
		2.8815392161530062,
	}
	if !reflect.DeepEqual(HeapSortFloat64(testarr), expect) {
		t.Fatal("HeapSort failed")
	}
}
