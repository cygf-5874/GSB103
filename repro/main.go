// Command repro 是最小复现：演示 fairsem 在多等待者下的两个可观察问题。
//
//	go run ./repro
//
// 它只用公开 API，不依赖 check/，方便在排查时快速看到现象。
package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"fairsem"
)

func main() {
	fmt.Println("== 复现 1：16 个等待者依次入队，观察授权顺序 ==")
	fifo()

	fmt.Println()
	fmt.Println("== 复现 2：并发释放突发，观察是否所有等待者都被唤醒 ==")
	burst()
}

func fifo() {
	const G = 16
	s := fairsem.New(1)
	_ = s.Acquire(context.Background())

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
			s.Release()
		}(i)
	}
	for i := 0; i < G; i++ {
		close(goAhead[i])
		deadline := time.Now().Add(2 * time.Second)
		for s.Stats().Waiting != i+1 && time.Now().Before(deadline) {
			runtime.Gosched()
		}
	}
	s.Release()

	order := make([]int, 0, G)
	for len(order) < G {
		select {
		case id := <-done:
			order = append(order, id)
		case <-time.After(3 * time.Second):
			fmt.Printf("  只完成 %d 个：%v\n", len(order), order)
			return
		}
	}
	fmt.Printf("  授权顺序：%v\n", order)
	fmt.Println("  期望（先进先出）：[0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15]")
}

func burst() {
	const (
		capN = 8
		G    = 64
	)
	s := fairsem.New(capN)
	for i := 0; i < capN; i++ {
		_ = s.Acquire(context.Background())
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
			s.Release()
		}(i)
	}
	for i := 0; i < G; i++ {
		close(goAhead[i])
		deadline := time.Now().Add(2 * time.Second)
		for s.Stats().Waiting != i+1 && time.Now().Before(deadline) {
			runtime.Gosched()
		}
	}
	release := make(chan struct{})
	for i := 0; i < capN; i++ {
		go func() { <-release; s.Release() }()
	}
	close(release)

	got := 0
	deadline := time.After(3 * time.Second)
	for got < G {
		select {
		case <-done:
			got++
		case <-deadline:
			fmt.Printf("  只被唤醒 %d/%d 个（%d 个永久阻塞）\n", got, G, G-got)
			return
		}
	}
	fmt.Printf("  被唤醒 %d/%d 个\n", got, G)
}
