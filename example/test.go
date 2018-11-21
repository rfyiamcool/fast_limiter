package main

import (
	"fmt"
	"time"

	"github.com/rfyiamcool/fast_limiter"
)

var (
	limiter = new(fastLimiter.LimitCtl)
	opt     = fastLimiter.Options{
		TagRules: map[string]int64{
			"tag1": 500,
			"tag2": 1000,
			"tag3": 200,
		},
		AddrPort:  "127.0.0.1:6379",
		Period:    1,
		MaxWaiter: 10,
	}
	tag1 = "tag1"
)

func main() {
	var err error

	limiter, err = fastLimiter.New("xiaorui.cc", opt)
	if err != nil {
		panic(err.Error())
	}

	err = limiter.Incrby(tag1)
	if err != nil {
		fmt.Println(err)
	}

	tagModel, err := limiter.DumpTag(tag1)
	fmt.Println(tagModel.LocalCounter)

	for index := 0; index < 200; index++ {
		limiter.Incrby(tag1)
	}
	// wait to sync redis
	time.Sleep(1 * time.Second)

	limiter.IncrbyBlock(tag1)
}
