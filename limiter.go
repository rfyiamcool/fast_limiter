package fastLimiter

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	firstDone = 1
	firstUndo = 0
)

var (
	ErrMaxValueInvalid        = errors.New("max value invalid, must > 0")
	ErrTagsNull               = errors.New("options tags null")
	ErrTagNotFoundWait        = errors.New("tags not found in wait queue pool")
	ErrTagNotFoundPool        = errors.New("tags not found in limit model pool")
	ErrBeyondMaxWaiter        = errors.New("beyond max waiter limit")
	ErrBeyondMaxLimitValue    = errors.New("beyond max limit value")
	ErrBeyondMaxLimitValCheck = errors.New("check; beyond max limit value")
)

func New(name string, opt Options) (*LimitCtl, error) {
	opt.init()
	err := opt.check()
	if err != nil {
		return nil, err
	}

	ctl := &LimitCtl{
		service: name,
		running: true,
		options: opt,
	}

	ctl.init()
	go ctl.bgSyncLimitData()
	go ctl.bgPurgeReset()

	return ctl, nil
}

type LimitCtl struct {
	// service name, redisKey contains it.
	service string

	// control backgroud groutine to exit
	running bool

	options Options

	// map slice
	lock sync.Mutex

	// counter
	incrlock sync.Mutex

	// wait queue per tag
	waitQueuePool  map[string]*waitQueue
	limitModelPool []map[string]*LimitModel

	redisClient *redis.Pool
	redisKeyTTL int
}

func (ctl *LimitCtl) SetLoggerFunc(logFunc func(string)) {
	logger = logFunc
}

func (ctl *LimitCtl) init() {
	if ctl.redisClient == nil {
		ctl.redisClient = initRedis(ctl.options)
	}

	ctl.waitQueuePool = ctl.makeWaitQueuePool()
	ctl.limitModelPool = ctl.makelimitModelPool()
	ctl.redisKeyTTL = 60 + ctl.options.Period

	logger = loggerFunc
}

func (ctl *LimitCtl) makeRedisKey() string {
	var mark int
	mark = int(time.Now().Unix()) / ctl.options.Period
	return fmt.Sprintf("counter_period(%d)s_name(%s)_(%d)", ctl.options.Period, ctl.service, mark)
}

func (ctl *LimitCtl) makelimitModelPool() []map[string]*LimitModel {
	// init pool
	poolCap := ctl.options.MaxlimitModelPool
	limitModelPool := make([]map[string]*LimitModel, poolCap, poolCap)

	// init detail
	for idx, _ := range limitModelPool {
		limitModelPool[idx] = make(map[string]*LimitModel, len(ctl.options.TagRules))
		for tagName, _ := range ctl.options.TagRules {
			limitModelPool[idx][tagName] = new(LimitModel)
		}
	}

	return limitModelPool
}

func (ctl *LimitCtl) makeWaitQueuePool() map[string]*waitQueue {
	// init pool
	waitQueuePool := make(map[string]*waitQueue, len(ctl.options.TagRules))

	// init detail
	for tagName, _ := range ctl.options.TagRules {
		waitQueuePool[tagName] = newWaitQueue(ctl.options.MaxWaiter)
	}

	return waitQueuePool
}

// stop bg goroutine
func (ctl *LimitCtl) Stop() {
	ctl.running = false
}

// sync redis counter
func (ctl *LimitCtl) bgSyncLimitData() {
	for ctl.running {
		time.Sleep(ctl.options.SyncInterval)

		redisClient := ctl.redisClient.Get()

		// get current shard
		shard := ctl.getNowShardInPool()
		// order, avoid map random
		tagList := make([]string, 0, len(shard))

		// transaction + pipeline
		redisClient.Send("MULTI")
		for tag, model := range shard {
			redisClient.Send("HINCRBY", ctl.makeRedisKey(), tag, model.localCounter-model.lastCounter)
			model.lastCounter = model.localCounter
			tagList = append(tagList, tag)

			// wake all waiter
			ctl.tryWakeupWorkers(tag, model)
		}
		redisClient.Send("expire", ctl.makeRedisKey(), ctl.redisKeyTTL)
		resp, err := redis.Int64s(redisClient.Do("EXEC"))
		if err != nil {
			redisClient.Close()
			logger("sync redis limit data failed, err: %s" + err.Error())
			continue
		}

		// update memory counter
		for idx, tag := range tagList {
			val := resp[idx]
			shard[tag].globalCounter = val
		}

		redisClient.Close()
	}
}

// reset before counter
func (ctl *LimitCtl) bgPurgeReset() {
	for ctl.running {
		time.Sleep(time.Duration(ctl.options.Period) * time.Second)

		shards := ctl.getBeforeShardsInPool()
		for _, shard := range shards {
			for _, model := range shard {
				model.reset()
			}
		}
	}
}

// add tag, copy on write
func (ctl *LimitCtl) AddTag(name string, value int64) {

	ctl.lock.Lock()
	defer ctl.lock.Unlock()

	// double check
	_, ok := ctl.options.TagRules[name]
	if ok {
		return
	}

	// add in ctl.options
	ctl.options.TagRules[name] = value

	// add in ctl.wait_queue
	newWaitQueuePool := ctl.makeWaitQueuePool()
	for tag, dto := range ctl.waitQueuePool {
		newWaitQueuePool[tag] = dto
	}
	ctl.waitQueuePool = newWaitQueuePool

	// replace new pool
	newLimitModelPool := ctl.makelimitModelPool()
	for idx, shard := range ctl.limitModelPool {
		for tagName, model := range shard {
			newLimitModelPool[idx][tagName] = model
		}
	}
	ctl.limitModelPool = newLimitModelPool
}

// del tag, copy on write
func (ctl *LimitCtl) DeleleTag() {
	// del in ctl.options
	// del in ctl.wait_queue

	// ...
}

// set sync interval
func (ctl *LimitCtl) SetSyncInterval(d time.Duration) {
	ctl.options.SyncInterval = d
}

// block
func (ctl *LimitCtl) IncrbyBlock(tag string) error {
	for {
		reportor, err := ctl.incrby(tag, true)

		if reportor == nil && err == nil {
			return nil
		}

		// already full, direct return
		if err != nil && (err == ErrBeyondMaxWaiter || err == ErrWaitQueueFull) {
			return err
		}
		if err != nil && err == ErrBeyondMaxLimitValCheck {
			continue
		}

		reportor.Wait()
	}
}

// no delay, not block
func (ctl *LimitCtl) Incrby(tag string) error {
	_, err := ctl.incrby(tag, false)
	return err
}

func (ctl *LimitCtl) incrby(tag string, block bool) (*waitEntry, error) {
	shard := ctl.getNowShardInPool()
	model, ok := shard[tag]
	if !ok {
		return nil, ErrTagNotFoundPool
	}

	maxValue, _ := ctl.options.TagRules[tag]
	if model.getLocalCounter() < maxValue && model.getGlobalCounter() < maxValue {
		resp := model.addLocalCounter()
		// double check
		if resp > maxValue {
			model.reduceLocalCounter()
			return nil, ErrBeyondMaxLimitValCheck
		}
		return nil, nil
	}

	if block {
		reportor, err := ctl.addWaiter(tag, model)
		return reportor, err
	} else {
		return nil, ErrBeyondMaxLimitValue
	}
}

func (ctl *LimitCtl) DumpStats() interface{} {
	// waitQueue
	// option
	// ...
	return nil
}

func (ctl *LimitCtl) DumpTag(tag string) (*LimitModel, error) {
	return ctl.getNowTagInPool(tag)
}

func (ctl *LimitCtl) Dump() map[string]*LimitModel {
	shard := ctl.getNowShardInPool()
	return shard
}

func (ctl *LimitCtl) DumpAll() []map[string]*LimitModel {
	return ctl.limitModelPool
}

func (ctl *LimitCtl) DumpTime(ts int) map[string]*LimitModel {
	shard := ctl.getTimeShardsInPool(ts)
	return shard
}

func (ctl *LimitCtl) DumpTimeRange() []map[string]*LimitModel {
	return nil
}

func (ctl *LimitCtl) getNowTagInPool(tag string) (*LimitModel, error) {
	shard := ctl.getNowShardInPool()
	model, ok := shard[tag]
	if !ok {
		return nil, ErrTagNotFoundPool
	}

	return model, nil
}

func (ctl *LimitCtl) addWaiter(tag string, model *LimitModel) (*waitEntry, error) {
	waitQ, ok := ctl.waitQueuePool[tag]
	if !ok {
		return nil, ErrTagNotFoundWait
	}

	if waitQ.Length() >= ctl.options.MaxWaiter {
		return nil, ErrBeyondMaxWaiter
	}

	// add wait queue
	reportor, err := waitQ.Add()
	return reportor, err
}

// wakeup in new time windows
func (ctl *LimitCtl) tryWakeupWorkers(tag string, model *LimitModel) {
	if model.first != 0 {
		return
	}

	ok := atomic.CompareAndSwapInt64(&model.first, firstUndo, firstDone)
	if !ok {
		return
	}

	waitQ, ok := ctl.waitQueuePool[tag]
	if !ok {
		return
	}

	// wake up all waiter
	waitQ.WakeupAll()
}

func (ctl *LimitCtl) getNowShardInPool() map[string]*LimitModel {
	cur := int(time.Now().Unix())
	shard := cur / ctl.options.Period % ctl.options.MaxlimitModelPool
	return ctl.limitModelPool[shard]
}

// before 3 shard
func (ctl *LimitCtl) getBeforeShardsInPool() []map[string]*LimitModel {
	cur := int(time.Now().Unix())
	shard := cur / ctl.options.Period % ctl.options.MaxlimitModelPool
	ago := 5
	count := 15
	if shard < count {
		shard = ctl.options.MaxlimitModelPool
	}

	models := make([]map[string]*LimitModel, count, count)
	for index := ago; index < count; index++ {
		models[index] = ctl.limitModelPool[shard-index]
	}

	return models
}

func (ctl *LimitCtl) getTimeShardsInPool(ts int) map[string]*LimitModel {
	shard := ts / ctl.options.Period % ctl.options.MaxlimitModelPool
	return ctl.limitModelPool[shard]
}

type LimitModel struct {
	first         int64 // 0 = none, 1 = used
	localCounter  int64 // local counter
	globalCounter int64 // sync redis counter to globalCounter
	lastCounter   int64 // counter sync to redis last time
}

func (l *LimitModel) reset() {
	l.first = 0
	l.globalCounter = 0
	l.localCounter = 0
	l.lastCounter = 0
}

func (l *LimitModel) getLocalCounter() int64 {
	return atomic.LoadInt64(&l.localCounter)
}

func (l *LimitModel) getGlobalCounter() int64 {
	return atomic.LoadInt64(&l.globalCounter)
}

func (l *LimitModel) addLocalCounter() int64 {
	return atomic.AddInt64(&l.localCounter, 1)
}

func (l *LimitModel) reduceLocalCounter() int64 {
	return atomic.AddInt64(&l.localCounter, -1)
}

func (l *LimitModel) addGlobalCounter() int64 {
	return atomic.AddInt64(&l.globalCounter, 1)
}

func NewOptions() Options {
	return Options{}
}

type Options struct {
	MaxWaiter int

	// tagName -> maxValue
	TagRules map[string]int64

	// for redis
	AddrPort string
	DB       int
	Password string

	// limit max value per period seconds
	Period int // time unit: second, default 1s

	MaxlimitModelPool int // default 60

	// sync redis limit data in per
	SyncInterval time.Duration
}

func (opt *Options) init() {
	if opt.MaxlimitModelPool == 0 {
		opt.MaxlimitModelPool = 60
	}

	if opt.Period == 0 {
		opt.Period = 1 // seconds
	}

	if opt.SyncInterval.Seconds() == 0 {
		opt.SyncInterval = 200 * time.Millisecond
	}

	if opt.MaxWaiter == 0 {
		opt.MaxWaiter = 10
	}
}

func (opt *Options) check() error {
	for _, maxValue := range opt.TagRules {
		if maxValue == 0 {
			return ErrMaxValueInvalid
		}
	}

	if len(opt.TagRules) == 0 {
		return ErrTagsNull
	}

	return nil
}

func hashToInt(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

var logger = loggerFunc

func loggerFunc(msg string) {
	log.Println(msg)
}
