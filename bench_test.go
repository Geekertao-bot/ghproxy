package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 基准可比性说明（重要）
//
// 旧实现（allowOld）与新实现（rateLimiter）在「窗口已满」时都走拒绝路径，
// 但两者的边界语义不同：
//   - 旧实现：判断条件为 len(entries[ip]) > limit，且判断发生在记录当前请求
//     之前，因此窗口内实际会放行 limit+1 次（历史边界偏差）。
//   - 新实现：判断条件为 len(times) >= limit，窗口内恰好放行 limit 次。
//
// 为了让新旧对比处于**同一状态**（同一路径、同一每-IP 记录条数），下面两套
// 基准统一预填 limit+1 条记录，使一次调用即落入「已满 → 拒绝」的稳态：
//   - 旧实现：len(entries[ip]) == limit+1 > limit → 返回 false，不再追加，
//     记录数保持 limit+1。
//   - 新实现：len(times) == limit+1 >= limit → 返回 false，记录数保持 limit+1。
// 因此两组基准测的都是 **拒绝路径的稳态成本**，且每-IP 记录条数一致（101），
// 可直接比较。
//
// 做这个统一的两个理由（原基准的问题）：
//   1. 原基准旧实现预填 limit 条，第 1 次调用会先走放行路径把记录推到 limit+1，
//      此后才进入与上面相同的稳态；即计时区间内混入了一次路径不同的调用。
//      注意其增长是**有界的**（稳定在 limit+1），并非无界增长。
//   2. 原基准新实现预填 limit 条即已拒绝，记录数为 limit；旧实现稳态为 limit+1。
//      两边记录数相差 1，不是严格同态。
// 统一后旧结果**不能直接沿用**：数字本身变化不大（见报告），但此前的对比在构造上
// 并非同态，无法据其宣称「同状态对比」。
//
// 注意：这**不是**放行路径的对比。放行路径仅在记录数未达上限时出现，且对同一
// 记录序列旧实现比新实现多放行一次（limit+1 vs limit，即历史边界偏差），两者
// 不存在可直接对比的同一放行状态。如需放行路径数据，必须单独说明状态与记录数，
// 不能与下表混用。
//
// 时钟口径：New / Old 两组都用注入的固定时间（同一 now），属于「纯算法」对比；
// NewRealClock 用生产入口 allow()（锁内采样 time.Now），衡量真实时钟开销，
// 与前面两组不可直接混比。
// ---------------------------------------------------------------------------

// allowOld 复刻优化前的限流实现，仅用于基准对比：
// 每个请求都加全局锁并遍历所有 IP，且从不删除过期 IP 键。
func allowOld(entries map[string][]time.Time, mu *sync.Mutex, ip string, limit int64, now time.Time) bool {
	mu.Lock()
	for k, times := range entries {
		var recent []time.Time
		for _, ts := range times {
			if now.Sub(ts) <= time.Minute {
				recent = append(recent, ts)
			}
		}
		entries[k] = recent
	}
	if limit > 0 && len(entries[ip]) > int(limit) {
		mu.Unlock()
		return false
	}
	entries[ip] = append(entries[ip], now)
	mu.Unlock()
	return true
}

func makeIPs(n int) []string {
	ips := make([]string, n)
	for i := range ips {
		ips[i] = fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xff, (i>>8)&0xff, i&0xff)
	}
	return ips
}

// primeRecords 是各 IP 预填的记录条数：limit+1，使新旧实现都直接进入稳态拒绝
// 路径，且每-IP 记录条数一致（见文件头部说明）。
func primeRecords(limit int64) int64 { return limit + 1 }

// workerStride 是并行基准中相邻 worker 的起始 IP 偏移步长。
// 原实现每个 worker 都从 i := 0 开始，导致大量 worker 争抢同一个 IP，
// 多 IP 场景实际退化成单 IP。给每个 worker 一个互不相同的起始偏移后，
// 多 IP 场景才能真正覆盖不同 IP 的访问分布。
// 取一个与常见 IP 数量互质的较大质数，使 worker 起始位置在 IP 空间上分散。
const workerStride = 1000003

// parallelStart 返回当前 worker 的起始索引，仅在每个 worker 启动时调用一次，
// 不进入 pb.Next() 循环，因此不引入每次请求的额外开销。
func parallelStart(seq *atomic.Uint64, ipCount int) int {
	return int(seq.Add(1)-1) * workerStride % ipCount
}

func benchNew(b *testing.B, ipCount int, limit int64) {
	l := newRateLimiter()
	l.sweepEvery = time.Hour // 排除回收开销，只衡量单请求处理成本
	ips := makeIPs(ipCount)
	now := time.Now()
	// 预填至 limit+1 条，使热路径处于「已满 → 拒绝」的稳态（见文件头部说明）。
	for _, ip := range ips {
		for j := int64(0); j < primeRecords(limit); j++ {
			l.allowAt(ip, limit, now)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	var seq atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		i := parallelStart(&seq, ipCount)
		for pb.Next() {
			l.allowAt(ips[i%ipCount], limit, now)
			i++
		}
	})
}

// benchNewRealClock 衡量生产入口（锁内采样 time.Now）的单请求成本。
// 时钟与 New/Old 两组不同，结果只能在这两者之间比较。
func benchNewRealClock(b *testing.B, ipCount int, limit int64) {
	l := newRateLimiter()
	l.sweepEvery = time.Hour
	ips := makeIPs(ipCount)

	b.ReportAllocs()
	b.ResetTimer()
	var seq atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		i := parallelStart(&seq, ipCount)
		for pb.Next() {
			l.allow(ips[i%ipCount], limit)
			i++
		}
	})
}

func benchOld(b *testing.B, ipCount int, limit int64) {
	entries := make(map[string][]time.Time, ipCount)
	var mu sync.Mutex
	ips := makeIPs(ipCount)
	now := time.Now()
	// 与 benchNew 保持同一状态：预填 limit+1 条 → 稳态拒绝路径。
	for _, ip := range ips {
		for j := int64(0); j < primeRecords(limit); j++ {
			entries[ip] = append(entries[ip], now)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	var seq atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		i := parallelStart(&seq, ipCount)
		for pb.Next() {
			allowOld(entries, &mu, ips[i%ipCount], limit, now)
			i++
		}
	})
}

func BenchmarkLimiterNew_SingleIP(b *testing.B) { benchNew(b, 1, 100) }
func BenchmarkLimiterNew_ManyIPs(b *testing.B)  { benchNew(b, 1000, 100) }
func BenchmarkLimiterOld_SingleIP(b *testing.B) { benchOld(b, 1, 100) }
func BenchmarkLimiterOld_ManyIPs(b *testing.B)  { benchOld(b, 1000, 100) }

func BenchmarkLimiterNewRealClock_SingleIP(b *testing.B) { benchNewRealClock(b, 1, 100) }
func BenchmarkLimiterNewRealClock_ManyIPs(b *testing.B)  { benchNewRealClock(b, 1000, 100) }

// fixSweepLimiter 构造一个「已填充 n 个 IP、且下一次 allow 必然触发回收」的限流器。
// 填充工作不计入任何计时区间：调用方需在 StopTimer 状态下调用它。
func fixSweepLimiter(n int, base time.Time) *rateLimiter {
	ips := makeIPs(n)
	l := newRateLimiter()
	l.sweepEvery = time.Hour // 填充期间不触发回收
	for _, ip := range ips {
		l.allowAt(ip, 10, base)
	}
	l.sweepEvery = 0 // 下一次 allow 立即触发 sweepLocked
	return l
}

// BenchmarkLimiterSweep 只衡量**一次回收操作**本身（5000 个过期 IP 的清理）。
//
// 构造限流器与填充 5000 个 IP 属于初始化成本，通过 StopTimer/StartTimer 排除在
// 计时区间之外；报告中的 ns/op、B/op、allocs/op 都只反映 sweepLocked 及其触发的
// 那一次 allow 调用本身，可以解释为「单次 sweep 的成本」。
func BenchmarkLimiterSweep(b *testing.B) {
	const n = 5000
	base := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		l := fixSweepLimiter(n, base)
		next := base.Add(2 * time.Minute)
		b.StartTimer()

		l.allowAt("192.168.0.1", 10, next) // 触发 sweepLocked
	}
}

// BenchmarkLimiterSweepWithFill 衡量**完整周期**：新建限流器 + 填充 5000 个 IP
// + 触发一次回收。它包含初始化与数据填充成本，因此**不能**当作纯回收耗时解释。
// 保留它是为了观察「填充 + 回收」整体开销随规模的变化。
func BenchmarkLimiterSweepWithFill(b *testing.B) {
	const n = 5000
	base := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := fixSweepLimiter(n, base)
		next := base.Add(2 * time.Minute)
		l.allowAt("192.168.0.1", 10, next)
	}
}
