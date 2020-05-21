package skiplist

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/shopspring/decimal"
)

var benchList *SkipList
var discard *Element

func init() {
	// Initialize a big SkipList for the Get() benchmark
	benchList = New(true)

	for i := 0; i <= 10000000; i++ {
		benchList.Set(decimal.NewFromFloat(float64(i)), [1]byte{})
	}

	// Display the sizes of our basic structs
	var sl SkipList
	var el Element
	fmt.Printf("Structure sizes: SkipList is %v, Element is %v bytes\n", unsafe.Sizeof(sl), unsafe.Sizeof(el))
}

func checkSanity(list *SkipList, t *testing.T) {
	// each level must be correctly ordered
	for k, v := range list.next {
		//t.Log("Level", k)

		if v == nil {
			continue
		}

		if k > len(v.next) {
			t.Fatal("first node's level must be no less than current level")
		}

		next := v
		cnt := 1

		for next.next[k] != nil {
			if !(next.next[k].key.GreaterThanOrEqual(next.key)) {
				t.Fatalf("next key value must be greater than prev key value. [next:%v] [prev:%v]", next.next[k].key, next.key)
			}

			if k > len(next.next) {
				t.Fatalf("node's level must be no less than current level. [cur:%v] [node:%v]", k, next.next)
			}

			next = next.next[k]
			cnt++
		}

		if k == 0 {
			if cnt != list.Length {
				t.Fatalf("list len must match the level 0 nodes count. [cur:%v] [level0:%v]", cnt, list.Length)
			}
		}
	}
}

func TestBasicIntCRUD(t *testing.T) {
	var list *SkipList

	list = New(true)

	list.Set(decimal.NewFromFloat(10), 1)
	list.Set(decimal.NewFromFloat(60), 2)
	list.Set(decimal.NewFromFloat(30), 3)
	list.Set(decimal.NewFromFloat(20), 4)
	list.Set(decimal.NewFromFloat(90), 5)
	checkSanity(list, t)

	list.Set(decimal.NewFromFloat(30), 9)
	checkSanity(list, t)

	list.Remove(decimal.Zero)
	list.Remove(decimal.NewFromFloat(20))
	checkSanity(list, t)

	v1 := list.Get(decimal.NewFromFloat(10))
	v2 := list.Get(decimal.NewFromFloat(60))
	v3 := list.Get(decimal.NewFromFloat(30))
	v4 := list.Get(decimal.NewFromFloat(20))
	v5 := list.Get(decimal.NewFromFloat(90))
	v6 := list.Get(decimal.NewFromFloat(0))

	if v1 == nil || v1.value.(int) != 1 || !v1.key.Equal(decimal.NewFromFloat(10)) {
		t.Fatal(`wrong "10" value (expected "1")`, v1)
	}

	if v2 == nil || v2.value.(int) != 2 {
		t.Fatal(`wrong "60" value (expected "2")`)
	}

	if v3 == nil || v3.value.(int) != 9 {
		t.Fatal(`wrong "30" value (expected "9")`)
	}

	if v4 != nil {
		t.Fatal(`found value for key "20", which should have been deleted`)
	}

	if v5 == nil || v5.value.(int) != 5 {
		t.Fatal(`wrong "90" value`)
	}

	if v6 != nil {
		t.Fatal(`found value for key "0", which should have been deleted`)
	}
}

func TestChangeLevel(t *testing.T) {
	var i float64
	list := New(true)

	if list.maxLevel != DefaultMaxLevel {
		t.Fatal("max level must equal default max value")
	}

	list = NewWithMaxLevel(4, true)
	if list.maxLevel != 4 {
		t.Fatal("wrong maxLevel (wanted 4)", list.maxLevel)
	}

	for i = 1; i <= 201; i++ {
		list.Set(decimal.NewFromFloat(i), i*10)
	}

	checkSanity(list, t)

	if list.Length != 201 {
		t.Fatal("wrong list length", list.Length)
	}

	for c := list.Front(); c != nil; c = c.Next() {
		if !c.key.Mul(decimal.NewFromFloat(10)).Equal(decimal.NewFromFloat(c.value.(float64))) {
			t.Fatal("wrong list element value")
		}
	}
}

func TestChangeProbability(t *testing.T) {
	list := New(true)

	if list.probability != DefaultProbability {
		t.Fatal("new lists should have P value = DefaultProbability")
	}

	list.SetProbability(0.5)
	if list.probability != 0.5 {
		t.Fatal("failed to set new list probability value: expected 0.5, got", list.probability)
	}
}

func TestConcurrency(t *testing.T) {
	list := New(true)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for i := 0; i < 100000; i++ {
			list.Set(decimal.NewFromFloat(float64(i)), i)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 100000; i++ {
			list.Get(decimal.NewFromFloat(float64(i)))
		}
		wg.Done()
	}()

	wg.Wait()
	if list.Length != 100000 {
		t.Fail()
	}
}

func BenchmarkIncSet(b *testing.B) {
	b.ReportAllocs()
	list := New(true)

	for i := 0; i < b.N; i++ {
		list.Set(decimal.NewFromFloat(float64(i)), [1]byte{})
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkIncGet(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := benchList.Get(decimal.NewFromFloat(float64(i)))
		if res == nil {
			b.Fatal("failed to Get an element that should exist")
		}
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkDecSet(b *testing.B) {
	b.ReportAllocs()
	list := New(true)

	for i := b.N; i > 0; i-- {
		list.Set(decimal.NewFromFloat(float64(i)), [1]byte{})
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkDecGet(b *testing.B) {
	b.ReportAllocs()
	for i := b.N; i > 0; i-- {
		res := benchList.Get(decimal.NewFromFloat(float64(i)))
		if res == nil {
			b.Fatal("failed to Get an element that should exist", i)
		}
	}

	b.SetBytes(int64(b.N))
}
