package distribute

import (
	"encoding/hex"
	"fmt"
	"github.com/gonum/stat"
	. "github.com/smartystreets/goconvey/convey"
	"math/rand"
	"testing"
)

func TestSegfault(t *testing.T) {

	// perl -Mblib -MAlgorithm::ConsistentHash::Ketama -wE 'my $ketama = Algorithm::ConsistentHash::Ketama->new(); $ketama->add_bucket( "r01", 100 ); $ketama->add_bucket( "r02", 100 ); my $key = $ketama->hash( pack "H*", "37292b669dd8f7c952cf79ca0dc6c5d7" ); say $key'

	buckets := []Bucket{{Label: "r01", Weight: 100}, {Label: "r02", Weight: 100}}
	k, _ := New(buckets, 4096)

	tests := []struct {
		key string
		b   string
	}{
		{"161c6d14dae73a874ac0aa0017fb8340", "r01"},
		{"37292b669dd8f7c952cf79ca0dc6c5d7", "r01"},
	}

	for _, tt := range tests {
		key, _ := hex.DecodeString(tt.key)
		b := k.Hash(string(key))
		if b != tt.b {
			t.Errorf("k.Hash(%v)=%v, want %v", tt.key, b, tt.b)
		}
	}

}

func TestConsistent(t *testing.T) {
	names := []string{
		"i-03f4a4cb6e87d5103",
		"i-060c2b413bfebc0c0",
		"i-0faac40bdb0ea040f",
		"i-0f9d9227a258c7490",
		"i-08d5e369e1ddd13fb",
		"i-0f0bbe2249c90e065",
		"i-0bb006152705b5308",
		"i-0128e7588c7f31138",
		"i-0a1f3889709c47698",
		"i-0e7091a3fcf84402d",
		"i-058e9739db78e75ae",
		"i-02bf3825ac20e8c9d",
	}

	Convey("test consistent", t, func() {
		var buckets []Bucket
		for _, name := range names[:3] {
			buckets = append(buckets, Bucket{Label: name, Weight: 100})
		}
		c, err := New(buckets, 4096)
		So(err, ShouldBeNil)

		Convey("test consistent", func() {
			traceIds := getTraceIDs()
			more := make(map[string]string, len(traceIds))
			results := make(map[string]int, len(names))
			insertAndPrint(traceIds, c, results, more)
			fmt.Println("=====")

			err = c.Add(names[3], 100)
			So(err, ShouldBeNil)

			var moved int
			newResults := make(map[string]int, len(names))
			newMore := make(map[string]string, len(traceIds))
			movedWeird := 0
			for _, v := range traceIds {
				k := c.Hash(v)
				newResults[k]++
				newMore[v] = k
				if more[v] != k {
					moved++
					if k != names[3] {
						movedWeird++
					}
				}
			}
			fmt.Println("new Results", len(newResults))
			for k, v := range newResults {
				fmt.Println(k, v)
			}
			fmt.Println("moved", moved, "weird", movedWeird)
			So(movedWeird, ShouldBeLessThan, 2)
			values := make([]float64, 0, len(newResults))
			for _, v := range newResults {
				values = append(values, float64(v))
			}
			mean, stddev := stat.MeanStdDev(values, nil)
			fmt.Println("mean", mean, "stddev", stddev, "%", stddev/mean*100)
			So(stddev/mean*100, ShouldBeLessThanOrEqualTo, 1)
		})
	})
}

func insertAndPrint(traceIds []string, c *Continuum, results map[string]int, more map[string]string) {
	for _, v := range traceIds {
		k := c.Hash(v)
		results[k]++
		more[v] = k
	}
	for k, v := range results {
		fmt.Println(k, v)
	}
}

func getTraceIDs() []string {
	var traceIds []string
	rand.Seed(7)
	for x := 0; x < 1000000; x++ {
		traceIDLow := fmt.Sprintf("%016x", rand.Uint32())
		traceIDHigh := fmt.Sprintf("%016x", rand.Uint32())
		traceIds = append(traceIds, traceIDHigh+traceIDLow)
	}
	return traceIds
}

func TestMisc(t *testing.T) {
	Convey("test bad", t, func() {
		c := &Continuum{}
		So(c.Add("", 100), ShouldNotBeNil)
		c, err := New([]Bucket{{Label: "foo"}, {Label: "foo"}}, 4096)
		So(err, ShouldNotBeNil)
	})
	Convey("test misc", t, func() {
		c, err := New([]Bucket{}, 4096)
		So(err, ShouldBeNil)
		x := c.Hash("thing")
		So(x, ShouldEqual, "")
		So(c.Size(), ShouldEqual, 0)
		Convey("test add/remove", func() {
			err := c.Add("one", 100)
			So(err, ShouldBeNil)
			So(c.Size(), ShouldEqual, 1)
			So(c.Hash("thing"), ShouldEqual, "one")
			err = c.Add("two", 100)
			So(err, ShouldBeNil)
			So(c.Hash("thing"), ShouldEqual, "two")
			err = c.Add("two", 100)
			So(err, ShouldNotBeNil)
			err = c.Remove("two")
			So(err, ShouldBeNil)
			So(c.Hash("thing"), ShouldEqual, "one")
			err = c.Remove("two")
			So(err, ShouldNotBeNil)
		})
	})
}

func BenchmarkConsistentKetama(b *testing.B) {
	names := []string{
		"i-03f4a4cb6e87d5103",
		"i-060c2b413bfebc0c0",
		"i-0faac40bdb0ea040f",
		"i-0f9d9227a258c7490",
		"i-08d5e369e1ddd13fb",
		"i-0f0bbe2249c90e065",
		"i-0bb006152705b5308",
		"i-0128e7588c7f31138",
		"i-0a1f3889709c47698",
		"i-0e7091a3fcf84402d",
		"i-058e9739db78e75ae",
		"i-02bf3825ac20e8c9d",
	}
	benchmarks := []struct {
		name     string
		replicas int
		hosts    int
	}{
		{"2^10-9", 1024, 9},
		{"2^12-9", 4096, 9}, // this seems pretty optimal
		{"2^13-9", 8192, 9},
		{"2^14-9", 16384, 9},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			var buckets []Bucket
			for _, name := range names[:bm.hosts] {
				buckets = append(buckets, Bucket{Label: name, Weight: 100})
			}
			c, err := New(buckets, bm.replicas)
			if err != nil {
				b.Fatal(err.Error())
			}
			results := make(map[string]int, bm.hosts)
			var traceIds []string
			for i := 0; i < b.N; i++ {
				traceIDLow := fmt.Sprintf("%016x", rand.Uint32())
				traceIDHigh := fmt.Sprintf("%016x", rand.Uint32())
				traceIds = append(traceIds, traceIDHigh+traceIDLow)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for _, t := range traceIds {
				r := c.Hash(t)
				results[r]++
			}
			vals := make([]float64, 0, bm.hosts)
			for _, v := range results {
				vals = append(vals, float64(v))
				//fmt.Println(k, v)
			}
			mean, stddev := stat.MeanStdDev(vals, nil)
			fmt.Println(bm.name, "stddev", stddev, "percent", (stddev/mean)*100)
		})
	}
}
