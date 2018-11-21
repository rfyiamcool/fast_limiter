package fastLimiter

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var (
	limiter = new(LimitCtl)
	opt     = Options{
		TagRules: map[string]int64{
			"tag1": 500,
			"tag2": 150,
			"tag3": 500,
		},
		AddrPort:  "127.0.0.1:6379",
		Period:    5,
		MaxWaiter: 10,
	}
	tag1 = "tag1"
)

func TestRun(t *testing.T) {
	var err error

	limiter, err = New("xiaorui.cc", opt)
	if err != nil {
		panic(err.Error())
	}

	err = limiter.Incrby(tag1)
	if err != nil {
		panic(err.Error())
	}

	tagModel, err := limiter.DumpTag(tag1)
	if tagModel.LocalCounter != 1 {
		panic("localCounter == 1")
	}

	for index := 0; index < 200; index++ {
		limiter.Incrby(tag1)
	}

	tagModel, _ = limiter.DumpTag(tag1)
	if tagModel.LocalCounter > opt.TagRules[tag1] {
		fmt.Printf("%+v \n", tagModel)
		panic("beyond max value")
	}

	time.Sleep(1 * time.Second)

	for index := 0; index < 110; index++ {
		limiter.Incrby(tag1)
	}
	limiter.IncrbyBlock(tag1)
}

func TestWhile(t *testing.T) {
	wg := sync.WaitGroup{}
	for index := 0; index < 150; index++ {
		wg.Add(1)
		go func(idx int) {
			limiter.IncrbyBlock(tag1)
			wg.Done()
		}(index)
	}
	wg.Wait()
}

func TestCluster(t *testing.T) {
	go func() {
		for {
			// data := limiter.DumpAll()
			// for idx, m := range data {
			// 	fmt.Println(idx)
			// 	for tag, dto := range m {
			// 		fmt.Printf("%s %+v \n", tag, dto)
			// 	}
			// }

			time.Sleep(1 * time.Second)
		}
	}()
	for index := 0; index < 100; index++ {
		fmt.Println("start: ", index)
		TestWhile(t)
	}
}
