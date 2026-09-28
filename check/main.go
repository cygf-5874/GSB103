// Command check 是 fairsem 的固定验收程序。
//
// ⚠️ 不要修改本文件。它是判定「题目有没有做对」的依据。
//
// 7 个场景分四组：
//
//	fifo   1：16 个等待者按进入顺序获得许可（严格先进先出）
//	wake   1：并发释放突发下不丢失唤醒，所有等待者都被唤醒
//	count  1：并发读写时计数快照自洽（Issued == Acquired + Waiting）
//	cancel 1：取消的等待者从队列摘除，且不吞掉许可
//	close  1：Close 唤醒所有等待者并幂等
//	race   2：取消与释放交错 / Close 与 Acquire 交错
//
// 并发场景都用栅栏把等待者凑齐再放行；每个场景在独立 goroutine 里跑，
// 带看门狗（超时记 FAIL），panic 被逐场景捕获记 FAIL 后继续，失败不早退。
//
// 用法：
//
//	go run ./check                 # 跑全部场景
//	go run ./check --only fifo     # 只跑一组
//	go run ./check -list           # 列出场景
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fairsem"
)

type scenario struct {
	group string
	name  string
	run   func() (string, string) // (期望, 实际)；相等即通过
}

var watchdog = 6 * time.Second

func eqInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fmtInts(a []int) string {
	parts := make([]string, len(a))
	for i, v := range a {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, " ")
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// spin 自旋等待 cond 成立；用 Gosched 让出，不靠 sleep 计时。
func spin(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return cond()
		}
		runtime.Gosched()
	}
}

// ------------------------------------------------------------------- fifo

// fifoOrder：n=1，主 goroutine 先持有一个许可；16 个等待者按 0..15 的顺序
// 逐个进入 Acquire（用 goAhead 栅栏串起来，保证入队顺序确定）。只释放一次，
// 之后由拿到许可者归还、把许可传给下一个 —— 授权被严格串行化，
// 因此「完成的顺序」就等于「授权的顺序」。
func fifoOrder() (string, string) {
	const G = 16
	s := fairsem.New(1)
	if err := s.Acquire(context.Background()); err != nil {
		return "主 goroutine 持有许可", "Acquire: " + err.Error()
	}

	done := make(chan int, G)
	goAhead := make([]chan struct{}, G)
	for i := range goAhead {
		goAhead[i] = make(chan struct{})
	}
	var wg sync.WaitGroup
	for i := 0; i < G; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-goAhead[id]
			if err := s.Acquire(context.Background()); err != nil {
				done <- -1
				return
			}
			done <- id
			s.Release()
		}(i)
	}

	for i := 0; i < G; i++ {
		close(goAhead[i])
		if !spin(func() bool { return s.Stats().Waiting == i+1 }) {
			return fmt.Sprintf("第 %d 个等待者入队", i+1),
				fmt.Sprintf("Waiting=%d（应有 %d 个）", s.Stats().Waiting, i+1)
		}
	}

	s.Release() // 只放行一次，之后靠归还链往下传

	order := make([]int, 0, G)
	for len(order) < G {
		select {
		case id := <-done:
			order = append(order, id)
		case <-time.After(5 * time.Second):
			return fmt.Sprintf("授权顺序 [%s]", fmtInts(seq(G))),
				fmt.Sprintf("只完成 %d 个：[%s]", len(order), fmtInts(order))
		}
	}
	wg.Wait()

	if !eqInts(order, seq(G)) {
		return fmt.Sprintf("授权顺序 [%s]", fmtInts(seq(G))), fmt.Sprintf("[%s]", fmtInts(order))
	}
	return "先进先出 " + fmtInts(seq(G)), "先进先出 " + fmtInts(order)
}

// ------------------------------------------------------------------- wake

// wakeBurst：n=8，主 goroutine 持有 8 个许可；64 个等待者入队后，
// 由 8 个 goroutine 并发释放 64 次（栅栏放行，突发）。每个等待者拿到许可后
// 持有不放，因此 64 次释放要覆盖 64 个等待者：所有等待者都必须被唤醒。
func wakeBurst() (string, string) {
	const (
		capN     = 8
		G        = 64
		releaser = 8
		perRel   = G / releaser
	)
	s := fairsem.New(capN)
	for i := 0; i < capN; i++ {
		if err := s.Acquire(context.Background()); err != nil {
			return "主 goroutine 持有 8 个许可", "Acquire: " + err.Error()
		}
	}

	done := make(chan int, G)
	goAhead := make([]chan struct{}, G)
	for i := range goAhead {
		goAhead[i] = make(chan struct{})
	}
	for i := 0; i < G; i++ {
		go func(id int) {
			<-goAhead[id]
			if err := s.Acquire(context.Background()); err != nil {
				done <- -1
				return
			}
			done <- id
		}(i)
	}
	for i := 0; i < G; i++ {
		close(goAhead[i])
		if !spin(func() bool { return s.Stats().Waiting == i+1 }) {
			return fmt.Sprintf("%d 个等待者入队", G),
				fmt.Sprintf("第 %d 个未入队（Waiting=%d）", i+1, s.Stats().Waiting)
		}
	}

	// 并发释放 64 次：突发，检验不丢唤醒。
	release := make(chan struct{})
	var rwg sync.WaitGroup
	for i := 0; i < releaser; i++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			<-release
			for j := 0; j < perRel; j++ {
				s.Release()
			}
		}()
	}
	close(release)
	rwg.Wait()

	got := 0
	deadline := time.After(watchdog)
	for got < G {
		select {
		case <-done:
			got++
		case <-deadline:
			return "64 个等待者全部被唤醒", fmt.Sprintf("只唤醒 %d 个（%d 个永久阻塞）", got, G-got)
		}
	}
	return "64 个等待者全部被唤醒", fmt.Sprintf("%d 个等待者全部被唤醒", got)
}

// ------------------------------------------------------------------ count

// countsConsistent：16 个 worker 反复 Acquire/Release，同时高频采样 Stats()，
// 任何一次采样都必须满足 0 <= Acquired <= n 且 Issued == Acquired + Waiting。
func countsConsistent() (string, string) {
	const (
		capN = 64
		W    = 16
		ITER = 50000
	)
	s := fairsem.New(capN)
	var start sync.WaitGroup
	var wg sync.WaitGroup
	start.Add(W)
	gate := make(chan struct{})
	var violation atomic.Value

	for w := 0; w < W; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Done()
			<-gate
			for i := 0; i < ITER; i++ {
				if err := s.Acquire(context.Background()); err != nil {
					return
				}
				s.Release()
			}
		}()
	}
	start.Wait()
	close(gate)

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()

	samples := 0
	for {
		select {
		case <-finished:
			if v := violation.Load(); v != nil {
				return "采样始终自洽", v.(string)
			}
			c := s.Stats()
			if c.Acquired != 0 || c.Waiting != 0 {
				return "全部归还后 Acquired=0、Waiting=0",
					fmt.Sprintf("Acquired=%d Waiting=%d", c.Acquired, c.Waiting)
			}
			return fmt.Sprintf("采样 %d 次全部自洽", samples),
				fmt.Sprintf("采样 %d 次全部自洽", samples)
		default:
		}
		c := s.Stats()
		samples++
		if c.Acquired < 0 || c.Acquired > capN || c.Waiting < 0 || c.Issued < 0 {
			violation.Store(fmt.Sprintf("第 %d 次采样越界 %+v", samples, c))
			continue
		}
		if c.Issued != c.Acquired+c.Waiting {
			violation.Store(fmt.Sprintf("第 %d 次采样 Issued(%d) != Acquired(%d)+Waiting(%d)",
				samples, c.Issued, c.Acquired, c.Waiting))
			continue
		}
	}
}

// ----------------------------------------------------------------- cancel

// cancelNoSwallow：n=1，主 goroutine 持有许可。一个等待者排队后被取消，
// 必须从队列摘除且不吞掉许可：之后主 goroutine 归还许可，新的等待者要能拿到。
func cancelNoSwallow() (string, string) {
	s := fairsem.New(1)
	if err := s.Acquire(context.Background()); err != nil {
		return "主 goroutine 持有许可", "Acquire: " + err.Error()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelGo := make(chan struct{})
	cancelDone := make(chan error, 1)
	go func() {
		<-cancelGo
		cancelDone <- s.Acquire(ctx)
	}()
	close(cancelGo)
	if !spin(func() bool { return s.Stats().Waiting == 1 }) {
		return "取消者入队", fmt.Sprintf("Waiting=%d", s.Stats().Waiting)
	}

	before := s.Stats().Issued
	cancel()
	select {
	case err := <-cancelDone:
		if !errors.Is(err, context.Canceled) {
			return "取消者返回 context.Canceled", fmt.Sprintf("返回 %v", err)
		}
	case <-time.After(5 * time.Second):
		return "取消者返回 context.Canceled", "取消后 5s 未返回"
	}
	after := s.Stats()
	if after.Waiting != 0 {
		return "取消后 Waiting=0", fmt.Sprintf("Waiting=%d", after.Waiting)
	}
	if after.Issued > before {
		return fmt.Sprintf("取消后 Issued 不增（<= %d）", before), fmt.Sprintf("Issued=%d", after.Issued)
	}

	// 归还许可，新等待者应当立刻拿到。
	s.Release()
	got := make(chan error, 1)
	go func() { got <- s.Acquire(context.Background()) }()
	select {
	case err := <-got:
		if err != nil {
			return "许可未被吞掉（新等待者拿到许可）", "Acquire: " + err.Error()
		}
	case <-time.After(5 * time.Second):
		return "许可未被吞掉（新等待者拿到许可）", "归还后 5s 仍拿不到许可（被取消者吞掉了）"
	}
	return "取消者已摘除、许可未被吞", "取消者已摘除、许可未被吞"
}

// ------------------------------------------------------------------ close

// closeWakesWaiters：n=1，主 goroutine 持有许可；8 个等待者入队后 Close，
// 所有等待者必须返回 ErrClosed；重复 Close 幂等；已持锁者仍可 Release。
func closeWakesWaiters() (string, string) {
	const G = 8
	s := fairsem.New(1)
	if err := s.Acquire(context.Background()); err != nil {
		return "主 goroutine 持有许可", "Acquire: " + err.Error()
	}

	results := make(chan error, G)
	goAhead := make([]chan struct{}, G)
	for i := range goAhead {
		goAhead[i] = make(chan struct{})
	}
	for i := 0; i < G; i++ {
		go func(id int) {
			<-goAhead[id]
			results <- s.Acquire(context.Background())
		}(i)
	}
	for i := 0; i < G; i++ {
		close(goAhead[i])
		if !spin(func() bool { return s.Stats().Waiting == i+1 }) {
			return fmt.Sprintf("%d 个等待者入队", G),
				fmt.Sprintf("第 %d 个未入队（Waiting=%d）", i+1, s.Stats().Waiting)
		}
	}

	s.Close()
	s.Close() // 幂等

	for i := 0; i < G; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, fairsem.ErrClosed) {
				return "Close 后所有等待者返回 ErrClosed", fmt.Sprintf("第 %d 个返回 %v", i+1, err)
			}
		case <-time.After(watchdog):
			return "Close 后所有等待者返回 ErrClosed",
				fmt.Sprintf("只返回 %d 个（%d 个永久阻塞）", i, G-i)
		}
	}
	s.Release() // 已持锁者仍可 Release
	if got := s.Stats().Acquired; got != 0 {
		return "Release 后 Acquired=0", strconv.Itoa(got)
	}
	return "Close 唤醒全部等待者且幂等", "Close 唤醒全部等待者且幂等"
}

// ------------------------------------------------------------------- race

// raceCancelVsRelease：取消与释放交错。多个带可取消 ctx 的等待者与多个
// 正常 Acquire/Release 的 worker 在栅栏后混跑，结束时必须全部返回，
// 且计数快照仍自洽、持有数归零。
func raceCancelVsRelease() (string, string) {
	const (
		capN    = 4
		CANCELS = 24
		WORKERS = 8
		ITER    = 20000
	)
	s := fairsem.New(capN)

	var wg sync.WaitGroup
	var gate sync.WaitGroup
	release := make(chan struct{})
	gate.Add(CANCELS + capN + WORKERS)

	// 取消者：入队后被取消，必须返回。
	for i := 0; i < CANCELS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			gate.Done()
			<-release
			done := make(chan struct{})
			go func() { defer close(done); _ = s.Acquire(ctx) }()
			cancel()
			<-done
		}()
	}
	// 持锁者：钉住一部分许可，制造真实排队。
	holder := make(chan struct{})
	for i := 0; i < capN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate.Done()
			<-release
			if s.Acquire(context.Background()) == nil {
				<-holder
				s.Release()
			}
		}()
	}
	// 正常 worker。
	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate.Done()
			<-release
			for j := 0; j < ITER; j++ {
				if err := s.Acquire(context.Background()); err != nil {
					return
				}
				s.Release()
			}
		}()
	}
	gate.Wait()
	close(release)

	// 放行后稍后松开拓扑锁并检查进程能收尾。
	go func() {
		runtime.Gosched()
		close(holder)
	}()

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(watchdog):
		return "取消/持有/正常 worker 全部返回", "有 goroutine 永久阻塞"
	}

	// 收尾后再单独检查一次快照自洽。
	c := s.Stats()
	if c.Issued != c.Acquired+c.Waiting {
		return "结束时 Issued==Acquired+Waiting",
			fmt.Sprintf("Issued=%d Acquired=%d Waiting=%d", c.Issued, c.Acquired, c.Waiting)
	}
	return "取消与释放交错后状态自洽", "取消与释放交错后状态自洽"
}

// raceCloseVsAcquire：先把等待者全部凑齐（栅栏），再让 Close 与若干 Release
// 并发发生；所有 Acquire 必须返回 nil 或 ErrClosed，不许永久阻塞。
func raceCloseVsAcquire() (string, string) {
	const (
		capN = 4
		G    = 32
	)
	s := fairsem.New(capN)
	for i := 0; i < capN; i++ {
		if err := s.Acquire(context.Background()); err != nil {
			return "主 goroutine 持有许可", "Acquire: " + err.Error()
		}
	}

	errs := make(chan error, G)
	goAhead := make([]chan struct{}, G)
	for i := range goAhead {
		goAhead[i] = make(chan struct{})
	}
	for i := 0; i < G; i++ {
		go func(id int) {
			<-goAhead[id]
			errs <- s.Acquire(context.Background())
		}(i)
	}
	for i := 0; i < G; i++ {
		close(goAhead[i])
		if !spin(func() bool { return s.Stats().Waiting == i+1 }) {
			return fmt.Sprintf("%d 个等待者入队", G),
				fmt.Sprintf("第 %d 个未入队（Waiting=%d）", i+1, s.Stats().Waiting)
		}
	}

	// 栅栏放行：Close 与 capN 次 Release 并发发生。
	var wg sync.WaitGroup
	release := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-release
		s.Close()
	}()
	for i := 0; i < capN; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-release
			s.Release()
		}()
	}
	close(release)
	wg.Wait()

	for i := 0; i < G; i++ {
		select {
		case err := <-errs:
			if err != nil && !errors.Is(err, fairsem.ErrClosed) {
				return "所有 Acquire 返回 nil 或 ErrClosed", "返回 " + err.Error()
			}
		case <-time.After(watchdog):
			return "所有 Acquire 返回 nil 或 ErrClosed",
				fmt.Sprintf("只返回 %d 个（%d 个永久阻塞）", i, G-i)
		}
	}
	return "Close 与 Acquire 交错后全部返回", "Close 与 Acquire 交错后全部返回"
}

// ------------------------------------------------------------------- main

func scenarios() []scenario {
	return []scenario{
		{"fifo", "fifo_order", fifoOrder},
		{"wake", "wake_burst", wakeBurst},
		{"count", "counts_consistent", countsConsistent},
		{"cancel", "cancel_no_swallow", cancelNoSwallow},
		{"close", "close_wakes_waiters", closeWakesWaiters},
		{"race", "cancel_vs_release", raceCancelVsRelease},
		{"race", "close_vs_acquire", raceCloseVsAcquire},
	}
}

func main() {
	list := false
	only := ""
	for i := 1; i < len(os.Args); i++ {
		a := os.Args[i]
		switch {
		case a == "-list":
			list = true
		case a == "--only":
			if i+1 < len(os.Args) {
				only = os.Args[i+1]
				i++
			}
		case strings.HasPrefix(a, "--only="):
			only = strings.TrimPrefix(a, "--only=")
		}
	}

	all := scenarios()
	if list {
		for _, s := range all {
			fmt.Printf("%s/%s\n", s.group, s.name)
		}
		return
	}

	passed, total := 0, 0
	for _, s := range all {
		if only != "" && s.group != only {
			continue
		}
		total++
		exp, act := runScenario(s)
		if exp == act {
			passed++
			fmt.Printf("PASS %s/%s\n", s.group, s.name)
		} else {
			fmt.Printf("FAIL %s/%s  期望=%s 实际=%s\n", s.group, s.name, exp, act)
		}
	}
	fmt.Printf("结果：通过 %d/%d\n", passed, total)
	if passed != total {
		os.Exit(1)
	}
}

// runScenario 在独立 goroutine 里跑一个场景，带看门狗与 panic 捕获。
func runScenario(s scenario) (exp, act string) {
	type result struct{ exp, act string }
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{"场景正常结束（不 panic）", fmt.Sprintf("panic: %v", r)}
			}
		}()
		e, a := s.run()
		ch <- result{e, a}
	}()
	select {
	case r := <-ch:
		return r.exp, r.act
	case <-time.After(watchdog):
		return "场景在 " + watchdog.String() + " 内结束", "超时（疑似死锁）"
	}
}
