# fairsem

容量固定的**公平信号量**（Go 1.24，仅标准库，`go.mod` 无 `require`）。

多个 goroutine 共用一个 `Semaphore`：`Acquire` 拿许可，`Release` 还许可。
容量为 `n` 时最多同时有 `n` 个持锁者，其余调用者按**先进先出**排队。

```go
s := fairsem.New(2)
if err := s.Acquire(ctx); err != nil { ... }
defer s.Release()
```

## 怎么跑

```bash
go build ./...            # 编译
go test ./...             # 既有用例
go run ./check            # 固定验收程序（7 个场景）
go run ./check -list      # 列出全部场景
go run ./check --only fifo
bash scripts/check.sh     # 薄封装：go run ./check "$@"
go run ./repro            # 最小复现脚本
```

前提：Go 1.24。`check/`、`scripts/` 是固定验收的一部分，**请勿修改**。

## 对外保证

下面 7 条是这个信号量的契约。**它们是契约，不是「当前行为」的转述。**

1. **成对与共享**：`Acquire(ctx context.Context) error` 与 `Release()` 成对使用；
   同一个 `Semaphore` 可以被多个 goroutine 共用。容量 `n` 是任何时刻持锁者的上界。
2. **FIFO 公平**：等待者按**进入 `Acquire` 的先后顺序**获得许可（严格先进先出）。
   先进入排队的等待者一定先拿到许可。
3. **不丢失唤醒**：`Release` 时**恰好**唤醒队首一个等待者 —— 既不广播（一次唤醒多个），
   也不漏唤（让等待者永久睡下去）。只要有等待者且有许可归还，就一定有等待者被唤醒。
4. **计数不撕裂**：`Stats() Counters` 返回 `Issued` / `Acquired` / `Waiting` 三个计数的
   **一致快照**，任意时刻读到都满足
   `0 <= Acquired <= n`、`Waiting >= 0` 且 `Issued == Acquired + Waiting`。
   其中 `Acquired` 是当前持锁者数，`Waiting` 是当前排队者数。
5. **关闭语义**：`Close()` 之后
   - 所有**已经在排队**的等待者立刻以 `ErrClosed` 返回；
   - 再进入 `Acquire` 的调用直接返回 `ErrClosed`；
   - 已经在持锁者仍可正常 `Release`；
   - 重复 `Close` 幂等（不 panic、不报错）。
6. **取消语义**：`ctx` 被取消时，等待中的 `Acquire` 以 `ctx.Err()` 返回，并且
   - 该等待者必须**从队列中摘除**（不再占用队列位置、不再被计入 `Waiting`）；
   - **不得吞掉许可**：取消本身不产生也不消耗许可，取消之后空闲的许可对后续
     `Acquire` 仍然可用。
7. **构造**：容量 `n <= 0` 时 `New(n)` panic（这是文档化行为）。

## 公开接口

```go
type Semaphore struct{ ... }
type Counters struct {
    Issued   int
    Acquired int
    Waiting  int
}

var ErrClosed error

func New(n int) *Semaphore
func (s *Semaphore) Acquire(ctx context.Context) error
func (s *Semaphore) Release()
func (s *Semaphore) Close()
func (s *Semaphore) Stats() Counters
```

对外 API 名与签名已定死：可以新增内部结构与文件，但不要改这些名字与签名。

## 固定件场景（7 个）

| 分组 | 场景 | 覆盖 |
| --- | --- | --- |
| `fifo` | `fifo_order` | 保证 2、3：16 个等待者按进入顺序获得许可 |
| `wake` | `wake_burst` | 保证 3：并发释放突发下不丢唤醒，所有等待者都被唤醒 |
| `count` | `counts_consistent` | 保证 4：并发读写时计数快照自洽 |
| `cancel` | `cancel_no_swallow` | 保证 6：取消者摘除队列、许可不被吞 |
| `close` | `close_wakes_waiters` | 保证 5：`Close` 唤醒全部等待者且幂等 |
| `race` | `cancel_vs_release` | 保证 1、4、6：取消与释放交错后状态自洽 |
| `race` | `close_vs_acquire` | 保证 1、5：`Close` 与 `Acquire` 交错后全部返回 |

`--only <组名>`（`fifo` / `wake` / `count` / `cancel` / `close` / `race`）可以只跑一组。
并发场景都用**栅栏**把等待者凑齐再放行，每个场景在独立 goroutine 里跑、带看门狗，
panic 被逐场景捕获记 `FAIL` 后继续，**失败不早退**。

## 目录

```
.
├── .gitattributes
├── .gitignore
├── go.mod               module fairsem
├── fairsem.go           实现
├── fairsem_test.go      既有用例
├── PROMPT.md            本题的 User Prompt（逐字）
├── check/main.go        固定验收程序（勿改）
├── repro/main.go        最小复现脚本
├── scripts/check.sh     薄封装：go run ./check "$@"
└── README.md
```
