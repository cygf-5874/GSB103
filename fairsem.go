// Package fairsem 实现一个容量固定的公平信号量。
//
// 语义逐条写在 README 的「对外保证」一节里；本文件是当前实现。
package fairsem

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed 在信号量 Close 之后，由无法继续获取许可的 Acquire 返回。
var ErrClosed = errors.New("fairsem: semaphore closed")

// waiter 是一个排队中的等待者。
type waiter struct {
	ready chan struct{}
}

// Semaphore 是容量为 n 的公平信号量，可被多个 goroutine 共用。
type Semaphore struct {
	n int

	mu       sync.Mutex
	acquired int
	waiting  int
	issued   int
	closed   bool

	queue map[*waiter]struct{}
	wake  chan struct{}
}

// Counters 是信号量表内计数的一次快照。
type Counters struct {
	Issued   int
	Acquired int
	Waiting  int
}

// New 构造容量为 n 的信号量。n <= 0 时 panic。
func New(n int) *Semaphore {
	if n <= 0 {
		panic("fairsem: capacity must be positive")
	}
	return &Semaphore{
		n:     n,
		queue: make(map[*waiter]struct{}),
		wake:  make(chan struct{}, 1),
	}
}

// Acquire 获取一个许可；必要时阻塞，直到获得许可、ctx 被取消或信号量关闭。
func (s *Semaphore) Acquire(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}

	if s.acquired < s.n && len(s.queue) == 0 {
		s.acquired++
		s.issued = s.acquired + s.waiting
		s.mu.Unlock()
		return nil
	}

	w := &waiter{ready: make(chan struct{})}
	s.queue[w] = struct{}{}
	s.waiting++
	s.issued = s.acquired + s.waiting
	s.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.wake:
			s.mu.Lock()
			var head *waiter
			for x := range s.queue {
				head = x
				break
			}
			if head != w {
				s.mu.Unlock()
				select {
				case s.wake <- struct{}{}:
				default:
				}
				continue
			}
			delete(s.queue, w)
			s.waiting--
			s.acquired++
			s.issued = s.acquired + s.waiting
			s.mu.Unlock()
			return nil
		}
	}
}

// Release 归还一个许可，并唤醒一名等待者。
func (s *Semaphore) Release() {
	s.mu.Lock()
	if s.acquired > 0 {
		s.acquired--
	}
	only := len(s.queue) == 1
	s.issued = s.acquired + s.waiting
	s.mu.Unlock()

	if only {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// Close 关闭信号量：其后再进入 Acquire 的调用直接返回 ErrClosed。
// 已在持锁者仍可 Release。重复 Close 幂等。
func (s *Semaphore) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
}

// Stats 返回计数快照。
func (s *Semaphore) Stats() Counters {
	return Counters{
		Issued:   s.issued,
		Acquired: s.acquired,
		Waiting:  s.waiting,
	}
}
