// Package fairsem 实现一个容量固定的公平信号量。
//
// 语义逐条写在 README 的「对外保证」一节里；本文件是当前实现。
package fairsem

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

// ErrClosed 在信号量 Close 之后，由无法继续获取许可的 Acquire 返回。
var ErrClosed = errors.New("fairsem: semaphore closed")

// 等待者在队列中的状态。
const (
	stWaiting = iota // 仍在排队等许可
	stGranted        // 已被 Release 授权
	stClosed         // 已被 Close 拒绝
)

// waiter 是一个排队中的等待者；每个等待者有自己的 ready 通道，
// 唤醒因此是点对点的，不存在「一个信号被错误的等待者消费」的问题。
type waiter struct {
	ready chan struct{}
	state int
	elem  *list.Element
}

// Semaphore 是容量为 n 的公平信号量，可被多个 goroutine 共用。
type Semaphore struct {
	n int

	mu       sync.Mutex
	acquired int
	closed   bool

	// queue 是等待者的 FIFO 队列，队首一定是下一个拿到许可的人。
	queue *list.List
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
		queue: list.New(),
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

	// 队列里没有等待者且有空闲许可：直接获得，不需要排队。
	if s.acquired < s.n && s.queue.Len() == 0 {
		s.acquired++
		s.mu.Unlock()
		return nil
	}

	w := &waiter{ready: make(chan struct{}), state: stWaiting}
	w.elem = s.queue.PushBack(w)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		// ctx 与唤醒可能同时就绪（select 随机选择），最终归属以锁内状态为准。
		s.mu.Lock()
		switch w.state {
		case stGranted:
			// 授权已经发给本等待者：不能吞这个许可，内部立刻归还，
			// 许可对后续 Acquire 依旧可用；对调用者仍按取消返回。
			s.releaseLocked()
			s.mu.Unlock()
			return ctx.Err()
		case stClosed:
			s.mu.Unlock()
			return ErrClosed
		default:
			s.queue.Remove(w.elem)
			s.mu.Unlock()
			return ctx.Err()
		}
	case <-w.ready:
		s.mu.Lock()
		state := w.state
		s.mu.Unlock()
		if state == stClosed {
			return ErrClosed
		}
		return nil
	}
}

// Release 归还一个许可，并恰好唤醒队首的一个等待者。
func (s *Semaphore) Release() {
	s.mu.Lock()
	s.releaseLocked()
	s.mu.Unlock()
}

// releaseLocked 是 Release 的锁内实现：有等待者时把许可直接转交给队首，
// 持锁者计数保持不变（一个许可从持锁者手上交给等待者）；没有等待者时
// 才真正回收一个许可。
func (s *Semaphore) releaseLocked() {
	if s.queue.Len() > 0 {
		w := s.queue.Remove(s.queue.Front()).(*waiter)
		w.state = stGranted
		close(w.ready)
		return
	}
	if s.acquired > 0 {
		s.acquired--
	}
}

// Close 关闭信号量：已经在排队的等待者立刻以 ErrClosed 返回，
// 其后再进入 Acquire 的调用直接返回 ErrClosed。
// 已在持锁者仍可 Release。重复 Close 幂等。
func (s *Semaphore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for s.queue.Len() > 0 {
		w := s.queue.Remove(s.queue.Front()).(*waiter)
		w.state = stClosed
		close(w.ready)
	}
}

// Stats 返回计数的一致快照：Issued == Acquired + Waiting。
func (s *Semaphore) Stats() Counters {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiting := s.queue.Len()
	return Counters{
		Issued:   s.acquired + waiting,
		Acquired: s.acquired,
		Waiting:  waiting,
	}
}
