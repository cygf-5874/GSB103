package fairsem

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// waitFor 轮询 cond 直到为真；超时则返回 false。只用于既有用例里的握手，
// 判据本身不依赖墙钟，超时值给得很宽松。
func waitFor(cond func() bool) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func TestNewPanicsOnNonPositive(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("New(%d) 应当 panic", n)
				}
			}()
			New(n)
		}()
	}
}

func TestAcquireImmediate(t *testing.T) {
	s := New(2)
	for i := 0; i < 2; i++ {
		if err := s.Acquire(context.Background()); err != nil {
			t.Fatalf("第 %d 次 Acquire 出错: %v", i+1, err)
		}
	}
	if got := s.Stats().Acquired; got != 2 {
		t.Fatalf("持有数应当为 2，得到 %d", got)
	}
}

func TestReleaseAllowsReacquire(t *testing.T) {
	s := New(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	s.Release()
	if got := s.Stats().Acquired; got != 0 {
		t.Fatalf("Release 后持有数应当为 0，得到 %d", got)
	}
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("重新 Acquire: %v", err)
	}
}

func TestStatsInvariantSingleThreaded(t *testing.T) {
	s := New(4)
	for i := 0; i < 100; i++ {
		if err := s.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		c := s.Stats()
		if c.Issued != c.Acquired+c.Waiting {
			t.Fatalf("Issued(%d) != Acquired(%d)+Waiting(%d)", c.Issued, c.Acquired, c.Waiting)
		}
		s.Release()
	}
}

func TestAcquireBlocksUntilRelease(t *testing.T) {
	s := New(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Acquire(context.Background()) }()
	if !waitFor(func() bool { return s.Stats().Waiting == 1 }) {
		t.Fatal("第二个 Acquire 应当进入等待")
	}
	s.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("等待中的 Acquire 应当成功: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待中的 Acquire 超时未返回")
	}
	if got := s.Stats().Acquired; got != 1 {
		t.Fatalf("转交后持有数应当为 1，得到 %d", got)
	}
}

func TestAcquireContextCancel(t *testing.T) {
	s := New(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Acquire(ctx) }()
	if !waitFor(func() bool { return s.Stats().Waiting == 1 }) {
		t.Fatal("取消前的 Acquire 应当进入等待")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("取消后应当返回 context.Canceled，得到 %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("取消后 Acquire 超时未返回")
	}
}

func TestAcquireAfterClose(t *testing.T) {
	s := New(1)
	s.Close()
	if err := s.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Close 后 Acquire 应当返回 ErrClosed，得到 %v", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	s := New(2)
	s.Close()
	s.Close()
	s.Close()
}

func TestHolderReleasesAfterClose(t *testing.T) {
	s := New(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	s.Close()
	s.Release()
	if got := s.Stats().Acquired; got != 0 {
		t.Fatalf("关闭后 Release 应当释放持有，得到持有数 %d", got)
	}
}

func TestCapacityRespected(t *testing.T) {
	s := New(3)
	for i := 0; i < 3; i++ {
		if err := s.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- s.Acquire(context.Background()) }()
	if !waitFor(func() bool { return s.Stats().Waiting == 1 }) {
		t.Fatal("容量占满后第四个 Acquire 应当等待")
	}
	s.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("释放后等待者应当成功: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("释放后等待者超时未返回")
	}
}

func TestTwoGoroutinesNoContention(t *testing.T) {
	s := New(2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := s.Acquire(context.Background()); err != nil {
				errs <- err
				return
			}
			s.Release()
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发 Acquire/Release 出错: %v", err)
	}
	if got := s.Stats().Acquired; got != 0 {
		t.Fatalf("全部释放后持有数应当为 0，得到 %d", got)
	}
}

func TestFencedSingleWaiterAlternation(t *testing.T) {
	s := New(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	for i := 0; i < 20; i++ {
		done := make(chan struct{})
		go func() {
			if err := s.Acquire(context.Background()); err != nil {
				t.Errorf("等待者 Acquire 出错: %v", err)
			}
			close(done)
		}()
		if !waitFor(func() bool { return s.Stats().Waiting == 1 }) {
			t.Fatal("等待者未进入队列")
		}
		s.Release()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("等待者超时未获得许可")
		}
	}
}
