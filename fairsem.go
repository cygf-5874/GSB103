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

// waiter 是一个排队中的等待者。
//
// 不变式：等待者要么在 queue 里（done == false），要么已经离队（done == true）
// 且 ch 里恰好被写入一个终态值（nil 表示获得许可，否则为失败原因）。
// ch 缓冲为 1，因此写入方永不阻塞。
type waiter struct {
	ctx  context.Context
	ch   chan error
	done bool
}

// Semaphore 是容量为 n 的公平信号量，可被多个 goroutine 共用。
type Semaphore struct {
	n int

	mu       sync.Mutex
	acquired int
	waiting  int
	closed   bool
	queue    *list.List // 元素为 *waiter，队首即最早进入的等待者
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

	if s.acquired < s.n && s.queue.Len() == 0 {
		s.acquired++
		s.mu.Unlock()
		return nil
	}

	w := &waiter{ctx: ctx, ch: make(chan error, 1)}
	elem := s.queue.PushBack(w)
	s.waiting++
	s.mu.Unlock()

	select {
	case err := <-w.ch:
		return err
	case <-ctx.Done():
		s.mu.Lock()
		if w.done {
			// 与 Release/Close 交错：终态值已经写入 ch，直接取用。
			s.mu.Unlock()
			return <-w.ch
		}
		// 仍在队列中：摘除自己，不占用队列位置，也不产生或消耗许可。
		w.done = true
		s.queue.Remove(elem)
		s.waiting--
		s.mu.Unlock()
		return ctx.Err()
	}
}

// Release 归还一个许可，并唤醒队首一名等待者。
func (s *Semaphore) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acquired == 0 {
		return
	}
	s.acquired--
	for s.queue.Len() > 0 {
		w := s.queue.Remove(s.queue.Front()).(*waiter)
		s.waiting--
		w.done = true
		if err := w.ctx.Err(); err != nil {
			// 已取消的等待者不消耗许可，把许可让给下一位。
			w.ch <- err
			continue
		}
		s.acquired++
		w.ch <- nil
		return
	}
}

// Close 关闭信号量：唤醒全部排队中的等待者（返回 ErrClosed），
// 其后再进入 Acquire 的调用直接返回 ErrClosed。
// 已在持锁者仍可 Release。重复 Close 幂等。
func (s *Semaphore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for e := s.queue.Front(); e != nil; e = e.Next() {
		w := e.Value.(*waiter)
		w.done = true
		w.ch <- ErrClosed
	}
	s.queue.Init()
	s.waiting = 0
}

// Stats 返回计数快照。三个计数在同一把锁下读出，且 Issued 由
// Acquired + Waiting 现算，任意时刻都满足 Issued == Acquired + Waiting。
func (s *Semaphore) Stats() Counters {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Counters{
		Issued:   s.acquired + s.waiting,
		Acquired: s.acquired,
		Waiting:  s.waiting,
	}
}
