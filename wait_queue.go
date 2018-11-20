package fastLimiter

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrWaitQueueFull = errors.New("wait queue full")
)

func newWaitQueue(maxLength int) *waitQueue {
	return &waitQueue{
		maxLength: maxLength,
		queue:     make(chan chan int, maxLength),
	}
}

type waitQueue struct {
	sync.Mutex
	maxLength int
	queue     chan chan int // optimize: array, start_idx, end_idx
}

func (w *waitQueue) Add() (*waitEntry, error) {
	if w.isFull() {
		return nil, ErrWaitQueueFull
	}

	pipe := make(chan int, 1) // fix atomic logic
	select {
	case w.queue <- pipe:
	default:
		return nil, ErrWaitQueueFull
	}

	reportor := newWaitEntry(pipe)
	return reportor, nil
}

func (w *waitQueue) isFull() bool {
	if len(w.queue) >= w.maxLength {
		return true
	}

	return false
}

func (w *waitQueue) Length() int {
	return len(w.queue)
}

func (w *waitQueue) Dump() {
}

func (w *waitQueue) WakeupAll() {
	for {
		select {
		case reportor, ok := <-w.queue:
			if !ok {
				return
			}
			w.send(reportor)

		default:
			// avoid block
			return
		}
	}
}

func (w *waitQueue) send(q chan int) {
	select {
	case q <- 1:
	default:
		// avoid block, maybe already full
	}
}

func (w *waitQueue) WakeupNum(num int) {
	cur := 0

	for reportor := range w.queue {
		select {
		case reportor <- 0:
		default:
			// maybe already full
		}

		cur++
		if cur == num {
			return
		}
	}
}

func newWaitEntry(buf chan int) *waitEntry {
	return &waitEntry{
		buf: buf,
	}
}

type waitEntry struct {
	buf chan int
}

func (w *waitEntry) Wait() {
	<-w.buf
}

func (w *waitEntry) WaitTimeout(d time.Duration) bool {
	select {
	case <-w.buf:
		return true
	case <-time.After(d):
		return false
	}
}
